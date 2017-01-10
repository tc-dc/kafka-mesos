/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.mesos.kafka.scheduler.mesos

import java.util
import java.util.Date
import ly.stealth.mesos.kafka.{Broker, RangeSet}
import net.elodina.mesos.util.Range
import org.apache.log4j.Logger
import org.apache.mesos.Protos.Resource.{DiskInfo, ReservationInfo}
import org.apache.mesos.Protos.Resource.DiskInfo.{Persistence, Source}
import org.apache.mesos.Protos.Volume.Mode
import org.apache.mesos.Protos.{Filters, Offer, OfferID, Resource, Value, Volume}
import scala.{Range => SRange}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random


abstract class OfferResult {
  def offer: Offer
}
object OfferResult {
  abstract class Decline extends OfferResult {
    def reason: String
    def duration: Int
  }

  case class DeclineBroker(
    offer: Offer,
    broker: Broker,
    reason: String,
    duration: Int = 5
  ) extends Decline
  case class NoWork(offer: Offer) extends Decline {
    val reason = "no work to do"
    val duration: Int = 60 * 60
  }
  case class Accept(offer: Offer, broker: Broker, reservation: Reservation) extends OfferResult

  def neverMatch(offer: Offer, broker: Broker, reason: String) =
    DeclineBroker(offer, broker, reason, 60 * 60)
  def eventuallyMatch(offer: Offer, broker: Broker, reason: String, howLong: Int) =
    DeclineBroker(offer, broker, reason, howLong)
}

class Reservation(
  role: String = null,
  sharedCpus: Double = 0.0,
  roleCpus: Double = 0.0,
  sharedMem: Long = 0,
  roleMem: Long = 0,
  val port: Option[Int],
  val jmxPort: Option[Int] = None,
  sharedPorts: Set[Int] = Set(),
  rolePorts: Set[Int] = Set(),
  val volume: String = null,
  volumeSize: Double = 0.0,
  volumePrincipal: String = null,
  val diskSource: Source = null
) {
  val cpus: Double = sharedCpus + roleCpus
  val mem: Long = sharedMem + roleMem

  def toResources: util.List[Resource] = {
    def cpus(value: Double, role: String): Resource = {
      Resource.newBuilder
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(value))
        .setRole(role)
        .build()
    }

    def mem(value: Long, role: String): Resource = {
      Resource.newBuilder
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(value))
        .setRole(role)
        .build()
    }

    def _port(value: Int): Resource = {
      val sourceRole = if (sharedPorts.contains(value)) "*" else role

      Resource.newBuilder
        .setName("ports")
        .setType(Value.Type.RANGES)
        .setRanges(Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(value).setEnd(value)))
        .setRole(sourceRole)
        .build()
    }

    def volumeDisk(id: String, value: Double, role: String, principal: String, diskSource: Source): Resource = {
      // TODO: add support for changing container path
      val volume = Volume.newBuilder.setMode(Mode.RW).setContainerPath("data").build()
      val persistence = Persistence.newBuilder.setId(id).build()

      val diskBuilder = DiskInfo.newBuilder
        .setPersistence(persistence)
        .setVolume(volume)
      if (diskSource != null && diskSource.hasType)
        diskBuilder.setSource(diskSource)

      val disk = diskBuilder.build()

      val reservation = ReservationInfo.newBuilder.setPrincipal(principal).build()
      Resource.newBuilder
        .setName("disk")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder.setValue(value))
        .setRole(role)
        .setDisk(disk)
        .setReservation(reservation)
        .build()
    }

    val resources: util.List[Resource] = new util.ArrayList[Resource]()

    if (sharedCpus > 0) resources.add(cpus(sharedCpus, "*"))
    if (roleCpus > 0) resources.add(cpus(roleCpus, role))

    if (sharedMem > 0) resources.add(mem(sharedMem, "*"))
    if (roleMem > 0) resources.add(mem(roleMem, role))

    Seq(port, jmxPort).flatten.foreach { p => resources.add(_port(p)) }

    if (volume != null) resources.add(volumeDisk(volume, volumeSize, role, volumePrincipal, diskSource))
    resources
  }
}

object OfferManager {
  def otherTasksAttributes(name: String, brokers: Iterable[Broker]): Iterable[String] = {
    def value(task: Broker.Task, name: String): Option[String] = {
      if (name == "hostname") return Some(task.hostname)
      task.attributes.get(name)
    }

    brokers.flatMap(b => Option(b.task).flatMap(t => value(t, name)))
  }

  def getReservation(broker: Broker, offer: Offer): Reservation = {
    var sharedCpus: Double = 0
    var roleCpus: Double = 0
    var reservedSharedCpus: Double = 0
    var reservedRoleCpus: Double = 0

    var sharedMem: Long = 0
    var roleMem: Long = 0
    var reservedSharedMem: Long = 0
    var reservedRoleMem: Long = 0

    var sharedPorts = new RangeSet()
    var rolePorts = new RangeSet()

    var role: String = null

    var reservedVolume: String = null
    var reservedVolumeSize: Double = 0
    var reservedVolumePrincipal: String = null
    var reservedVolumeSource: Source = null

    for (resource <- offer.getResourcesList) {
      if (resource.getRole == "*") {
        // shared resources
        if (resource.getName == "cpus") sharedCpus = resource.getScalar.getValue
        if (resource.getName == "mem") sharedMem = resource.getScalar.getValue.toLong
        if (resource.getName == "ports") sharedPorts =
          new RangeSet(resource.getRanges.getRangeList.map(r => SRange.inclusive(r.getBegin.toInt, r.getEnd.toInt)))
      } else {
        if (role != null && role != resource.getRole)
          throw new IllegalArgumentException(s"Offer contains 2 non-default roles: $role, ${resource.getRole}")
        role = resource.getRole

        // static role-reserved resources
        if (!resource.hasReservation) {
          if (resource.getName == "cpus") roleCpus = resource.getScalar.getValue
          if (resource.getName == "mem") roleMem = resource.getScalar.getValue.toLong
          if (resource.getName == "ports") rolePorts =
            new RangeSet(resource.getRanges.getRangeList.map(r => SRange.inclusive(r.getBegin.toInt, r.getEnd.toInt)))
        }

        // dynamic role/principal-reserved volume
        if (broker.volume != null && resource.hasDisk && resource.getDisk.hasPersistence && resource.getDisk.getPersistence.getId == broker.volume) {
          reservedVolume = broker.volume
          reservedVolumeSize = resource.getScalar.getValue
          reservedVolumePrincipal = resource.getReservation.getPrincipal
          // will be NULL for root volumes
          reservedVolumeSource = resource.getDisk.getSource
        }
      }
    }

    reservedRoleCpus = Math.min(broker.cpus, roleCpus)
    reservedSharedCpus = Math.min(broker.cpus - reservedRoleCpus, sharedCpus)

    reservedRoleMem = Math.min(broker.mem, roleMem)
    reservedSharedMem = Math.min(broker.mem - reservedRoleMem, sharedMem)

    val ports = rolePorts | sharedPorts
    val port = getSuitablePort(ports, broker.port)
    val jmxPort = (Option(broker.jmxPort), port) match {
      case (Some(jmxRange), Some(brokerPort)) => getSuitablePort(ports - brokerPort, jmxRange)
      case _ => None
    }

    new Reservation(role,
      reservedSharedCpus, reservedRoleCpus,
      reservedSharedMem, reservedRoleMem,
      port, jmxPort, sharedPorts, rolePorts,
      reservedVolume, reservedVolumeSize,
      reservedVolumePrincipal, reservedVolumeSource
    )
  }

  private[kafka] def getSuitablePort(offerPorts: Set[Int], brokerPorts: Range): Option[Int] = {
    (if (offerPorts.isEmpty) {
      Seq()
    }
    else if (brokerPorts != null) {
      offerPorts & new RangeSet(Seq(SRange.inclusive(brokerPorts.start, brokerPorts.end)))
    }
    else {
      offerPorts
    }).headOption
  }

  def brokerMatches(broker: Broker, offer: Offer, now: Date = new Date(), otherAttributes: Broker.OtherAttributes = Broker.NoAttributes): OfferResult = {
    // check resources
    val reservation = getReservation(broker, offer)
    if (reservation.cpus < broker.cpus) return OfferResult.neverMatch(offer, broker, s"cpus < ${broker.cpus}")
    if (reservation.mem < broker.mem) return OfferResult.neverMatch(offer, broker, s"mem < ${broker.mem}")
    if (reservation.port.isEmpty) return OfferResult.neverMatch(offer, broker, "no suitable port")
    if (reservation.jmxPort.isEmpty && broker.jmxPort != null) return OfferResult.neverMatch(offer, broker, "no suitable jmx port")

    // check volume
    if (broker.volume != null && reservation.volume == null)
      return OfferResult.neverMatch(offer, broker, s"offer missing volume: ${broker.volume}")

    // check attributes
    val offerAttributes = new util.HashMap[String, String]()
    offerAttributes.put("hostname", offer.getHostname)

    for (attribute <- offer.getAttributesList)
      if (attribute.hasText) offerAttributes.put(attribute.getName, attribute.getText.getValue)

    // check constraints
    for ((name, constraint) <- broker.constraints) {
      if (!offerAttributes.containsKey(name)) return OfferResult.neverMatch(offer, broker, s"no $name")
      if (!constraint.matches(offerAttributes.get(name), otherAttributes(name)))
        return OfferResult.neverMatch(offer, broker, s"$name doesn't match $constraint")
    }

    // check stickiness
    val stickyTimeLeft = broker.stickiness.stickyTimeLeft(now)
    if (stickyTimeLeft > 0)
      if (!broker.stickiness.matchesHostname(offer.getHostname))
        return OfferResult.eventuallyMatch(offer, broker, "hostname != stickiness host", stickyTimeLeft)

    // check failover delay
    val failoverDelay = (broker.failover.delayExpires.getTime - now.getTime) / 1000
    if (failoverDelay > 0) {
      return OfferResult.eventuallyMatch(offer, broker, "waiting to restart", failoverDelay.toInt)
    }

    // Accept it
    OfferResult.Accept(offer, broker, reservation)
  }

}

trait OfferManagerComponent {
  val offerManager: OfferManager

  trait OfferManager {
    def enableOfferSuppression()
    def tryAcceptOffer(
      offer: Offer,
      brokers: Iterable[Broker]
    ): Either[OfferResult.Accept, Seq[OfferResult.Decline]]
    def pauseOrResumeOffers(forceRevive: Boolean = false): Unit
    def declineOffer(offer: OfferID): Unit
    def declineOffer(offer: OfferID, filters: Filters): Unit
  }
}

trait OfferManagerComponentImpl extends OfferManagerComponent {
  this: ClusterComponent with SchedulerDriverComponent =>

  val offerManager: OfferManager = new OfferManagerImpl

  class OfferManagerImpl extends OfferManager {
    private[this] var offersAreSuppressed: Boolean = false
    private[this] val logger = Logger.getLogger(classOf[OfferManager])
    private[this] var canSuppressOffers = true

    def enableOfferSuppression(): Unit = canSuppressOffers = true

    def tryAcceptOffer(
      offer: Offer,
      brokers: Iterable[Broker]
    ): Either[OfferResult.Accept, Seq[OfferResult.Decline]] = {
      val now = new Date()
      val declines = mutable.Buffer[OfferResult.Decline]()

      for (broker <- brokers.filter(_.shouldStart(offer.getHostname))) {
        OfferManager.brokerMatches(broker, offer, now, name => OfferManager.otherTasksAttributes(name, brokers)) match {
          case accept: OfferResult.Accept => return Left(accept)
          case reason: OfferResult.Decline => declines.append(reason)
        }
      }

      // if we got here we're declining the offer,
      // if there's no reason it just meant we had nothing to do
      if (declines.isEmpty)
        declines.append(OfferResult.NoWork(offer))

      val maxDuration = declines.map(_.duration).max.toDouble
      val jitter = (maxDuration / 3) * (Random.nextDouble() - .5)
      val fb = Filters.newBuilder().setRefuseSeconds(maxDuration + jitter)
      declineOffer(offer.getId, fb.build())
      Right(declines)
    }

    def declineOffer(offer: OfferID): Unit = Driver.call(_.declineOffer(offer))
    def declineOffer(offer: OfferID, filters: Filters): Unit =
      Driver.call(_.declineOffer(offer, filters))

    def pauseOrResumeOffers(forceRevive: Boolean = false): Unit = {
      if (forceRevive) {
        driver.reviveOffers()
        logger.info("Re-requesting previously suppressed offers.")
        this.offersAreSuppressed = false
        return
      }

      val clusterIsSteadyState = cluster.getBrokers.asScala.forall(_.isSteadyState)
      // If all brokers are steady state we can request mesos to stop sending offers.
      if (!this.offersAreSuppressed && clusterIsSteadyState) {
        if (canSuppressOffers) {
          // Our version of mesos supports suppressOffers, so use it.
          Driver.call(_.suppressOffers())
          logger.info("Cluster is now stable, offers are suppressed")
          this.offersAreSuppressed = true
        }
        else {
          // No support for suppress offers, noop it.
          this.offersAreSuppressed = true
        }
      }
      // Else, if offers are suppressed, and we are no longer steady-state, resume offers.
      else if (!clusterIsSteadyState && offersAreSuppressed) {
        Driver.call(_.reviveOffers())
        logger.info("Cluster is no longer stable, resuming offers.")
        this.offersAreSuppressed = false
      }
    }
  }

}