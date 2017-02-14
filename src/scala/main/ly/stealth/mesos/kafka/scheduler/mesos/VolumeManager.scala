package ly.stealth.mesos.kafka.scheduler.mesos

import java.util.Date
import ly.stealth.mesos.kafka.{Broker, ConfigComponent}
import ly.stealth.mesos.kafka.Broker.{CreatedVolume, DynamicVolume, DynamicVolumeState, PendingVolume}
import org.apache.log4j.Logger
import org.apache.mesos.Protos.{Filters, Offer, Resource, Value, Volume => MesosVolume}
import org.apache.mesos.Protos.Offer.Operation
import org.apache.mesos.Protos.Resource.DiskInfo.Source
import scala.collection.mutable
import scala.collection.JavaConversions._

case class VolumeReservation(volume: DynamicVolume, resource: Resource)

abstract class FailureReason {
  val description: String
}
object FailureReason {
  val InsufficientVolumes = new FailureReason {
    val description = "not enough volumes in offer"
  }
  val ExistingVolumesNotInOffer = new FailureReason {
    val description = "existing volumes were not in offer"
  }
  val NoMatchingBrokers = new FailureReason {
    override val description: String = "no brokers matched offer"
  }
  val PartialMatch = new FailureReason {
    override val description: String = "unable to match all volumes to offer"
  }
}

abstract class CreateVolumes {
  def offer: Offer
}
object CreateVolumes {
  case class Success(offer: Offer, broker: Broker, reservations: Seq[VolumeReservation]) extends CreateVolumes
  case class Failure(offer: Offer, reason: FailureReason) extends CreateVolumes
}

trait VolumeManagerComponent {
  val volumeManager: VolumeManager

  trait VolumeManager {
    def tryCreateVolumes(offer: Offer, brokers: Iterable[Broker]): CreateVolumes
  }
}

trait VolumeManagerComponentImpl extends VolumeManagerComponent {
  this: SchedulerDriverComponent
    with ConfigComponent =>

  val volumeManager: VolumeManager = new VolumeManagerImpl

  class VolumeManagerImpl extends VolumeManager {
    private[this] val logger = Logger.getLogger("VolumeManager")

    def tryCreateVolumes(offer: Offer, brokers: Iterable[Broker]): CreateVolumes = {
      val now = new Date()
      // .view required to make this lazy evaluated
      brokers.view
        .filter(_.shouldStart(offer.getHostname))
        .map(_.matches(offer, now, name => OfferManager.otherTasksAttributes(name, brokers)))
        .collect {
          case OfferResult.DeclineMissingVolumes(o, b) => tryCreateVolumes(o, b)
        }
        .collect {
          case c: CreateVolumes.Success =>
            createVolume(c)
            Some(c.asInstanceOf[CreateVolumes])
          case CreateVolumes.Failure(o, reason) =>
            logger.debug(s"Failued to create volume from ${o.getId.getValue}: ${reason.description}")
            None
        }
        .flatten
        .headOption
        .getOrElse(CreateVolumes.Failure(offer, FailureReason.NoMatchingBrokers))
    }

    private def tryCreateVolumes(offer: Offer, broker: Broker): CreateVolumes = {
      val existingVolumes = broker.volumes.collect {
        case CreatedVolume(v) => v.volumeId
      }.toSet
      val offerVolumes = offer.getResourcesList
        .filter(o => o.hasDisk && o.getDisk.hasPersistence && o.getDisk.getPersistence.hasId)
        .map(_.getDisk.getPersistence.getId)
        .toSet

      // Failure, all volumes must be in the same offer.
      if (existingVolumes.exists(c => !offerVolumes.contains(c))) {
        return CreateVolumes.Failure(offer, FailureReason.ExistingVolumesNotInOffer)
      }

      val resourceCandidates = offer.getResourcesList
        .filter(
          r => r.hasRole &&
            r.getName == "disk" &&
            !diskResourceIsUsed(r))
        .sortBy(_.getScalar.getValue)

      val pendingVolumes = broker.volumes.collect {
        case PendingVolume(v) => v
      }.sortBy(_.sizeMb).toBuffer

      // Obviously this will never work
      if (pendingVolumes.length > resourceCandidates.length) {
        return CreateVolumes.Failure(offer, FailureReason.InsufficientVolumes)
      }

      val createdVolumes = mutable.Buffer[VolumeReservation]()
      while(resourceCandidates.nonEmpty && pendingVolumes.nonEmpty) {
        tryMatchBrokerAndOffer(resourceCandidates, pendingVolumes, broker) match {
          case Some((oc, pv)) =>
            resourceCandidates -= oc
            pendingVolumes -= pv
            createdVolumes.append(createVolumeReservation(pv, oc))
          case None =>
        }
      }

      // We successfully created all volumes on this broker, great success!
      if (createdVolumes.nonEmpty && pendingVolumes.isEmpty) {
        CreateVolumes.Success(offer, broker, createdVolumes)
      } else {
        CreateVolumes.Failure(offer, FailureReason.PartialMatch)
      }
    }

    private def resourceMatchesVolume(r: Resource, pv: DynamicVolume): Boolean = {
      if (pv.allowedSources.nonEmpty && !pv.allowedSources.contains(r.getDisk.getSource.getType)) {
        return false
      }

      // MOUNT disks must be an exact match
      if (r.hasDisk && r.getDisk.hasSource && r.getDisk.getSource.getType == Source.Type.MOUNT) {
        r.getScalar.getValue == pv.sizeMb
      } else {
        r.getScalar.getValue >= pv.sizeMb
      }
    }

    private def tryMatchBrokerAndOffer(
      sortedResources: Iterable[Resource],
      pendingVolumes: Seq[DynamicVolume],
      broker: Broker
    ) : Option[(Resource, DynamicVolume)] =
    {
      pendingVolumes.view.flatMap { pv =>
        sortedResources.find { r => resourceMatchesVolume(r, pv) }.map((_, pv))
      }.headOption
    }

    private def createVolume(success: CreateVolumes.Success) = {
      val op = Operation.newBuilder()
        .setCreate(Operation.Create.newBuilder().addAllVolumes(success.reservations.map(_.resource)))
        .setType(Operation.Type.CREATE)
        .build()

      val idsToReplace = success.reservations.map(_.volume.volumeId).toSet
      success.broker.volumes = success.broker.volumes
        .filterNot(v => idsToReplace.contains(v.volumeId)) ++ success.reservations.map(_.volume)

      logger.info(
        s"Using offer ${success.offer.getId.getValue} to create volume(s): " +
        success.reservations.map(_.volume).mkString(", "))

      if (logger.isDebugEnabled) {
        logger.debug(
          s"Offer -> ${success.offer.toString}\n" +
          s"Operation -> ${op.toString}")
      }

      Driver.call(_.acceptOffers(
        Seq(success.offer.getId),
        Seq(op),
        Filters.newBuilder().build()))
    }

    private def diskResourceIsUsed(r: Resource): Boolean = {
      if (!r.hasDisk) false
      else if (!r.getDisk.hasPersistence) false
      else if (!r.getDisk.getPersistence.hasId) false
      else true
    }

    private def createVolumeReservation(dv: DynamicVolume, res: Resource) = {
      val newVolume = dv.copy(state = DynamicVolumeState.Creating)
      val newResource = Resource.newBuilder(res)
      newResource.setScalar(Value.Scalar.newBuilder().setValue(newVolume.sizeMb))
      newResource.getDiskBuilder.getPersistenceBuilder.setId(newVolume.volumeId)
      newResource.getDiskBuilder.getVolumeBuilder
        .setContainerPath(newVolume.containerPath)
        .setMode(MesosVolume.Mode.RW)

      Option(config.principal).foreach { p =>
        newResource.getDiskBuilder.getPersistenceBuilder.setPrincipal(p)
      }

      VolumeReservation(newVolume, newResource.build())
    }
  }
}
