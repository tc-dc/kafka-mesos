package ly.stealth.mesos.kafka


import org.junit.Test
import org.junit.Assert._
import org.apache.mesos.Protos._
import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit
import ly.stealth.mesos.kafka.Broker.{Volume => _, _}
import ly.stealth.mesos.kafka.executor.{Executor, LaunchConfig}
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.BrokerState
import ly.stealth.mesos.kafka.scheduler.mesos.{CreateVolumes, OfferManager, OfferResult}
import net.elodina.mesos.util.Period
import net.elodina.mesos.util.Strings.parseMap
import org.apache.mesos.Protos.Offer.Operation
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration

class VolumeTest extends KafkaMesosTestCase {
  private def waitForEventLoop() = {
    registry.eventLoop.submit(new Runnable { override def run(): Unit = {} }).get()
  }

  @Test
  def createSingleVolume: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(DynamicVolume(1000, "data", "test"))
    val broker2 = registry.cluster.addBroker(new Broker(2))
    broker2.active = true

    val o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000; disk(test):20000")
    val result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker2, broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Success])
    val created = result.asInstanceOf[CreateVolumes.Success]

    assertEquals(0, created.broker.id)
    assertEquals(o1.getId, created.offer.getId)
    assertEquals(DynamicVolumeState.Creating, broker.volumes.head.asInstanceOf[DynamicVolume].state)
    val btsd = schedulerDriver.asInstanceOf[BetterTestSchedulerDriver]
    assertEquals(1, btsd.fullAcceptedOffers.length)
    val (_, acceptedOffer) = btsd.fullAcceptedOffers.head
    assertEquals(Operation.Type.CREATE, acceptedOffer.head.getType)
    assertEquals(1000, acceptedOffer.head.getCreate.getVolumes(0).getScalar.getValue, 0)
    assertEquals("data", acceptedOffer.head.getCreate.getVolumes(0).getDisk.getVolume.getContainerPath)
  }

  @Test
  def createVolumeUsesBestSizeMatch: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(DynamicVolume(100, "data", "test"))

    val o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10; disk(test):20000; disk(test):100")
    val o2 = offer("cpus:1; mem:2048; ports:1000; disk(test):10")
    val result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Success])
    val created = result.asInstanceOf[CreateVolumes.Success]
    assertEquals(100, created.reservations.head.resource.getScalar.getValue, 0)
  }

  @Test
  def createMultipleVolumes_Success: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "test"),
      DynamicVolume(1000, "data1", "test1")
    )

    var o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000; disk(test):10000")
    var result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Success])
    var created = result.asInstanceOf[CreateVolumes.Success]

    assertEquals(0, created.broker.id)
    assertEquals(o1.getId, created.offer.getId)
    assertEquals(2, created.reservations.length)

    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "volume-1", DynamicVolumeState.Ready),
      DynamicVolume(1000, "data1", "volume-2")
    )

    // Add a new volume to an existing offer
    o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000; disk(test)[volume-1:data]:10000")
    result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Success])
    created = result.asInstanceOf[CreateVolumes.Success]
  }

  @Test
  def createMultipleVolumes_Failure: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "test1"),
      DynamicVolume(1000, "data1", "test2")
    )

    var o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000")
    var result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Failure])

    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "volume-1", DynamicVolumeState.Ready),
      DynamicVolume(1000, "data1", "volume-2")
    )

    o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000")
    result = registry.volumeManager.tryCreateVolumes(o1, Seq(broker))
    assertTrue(result.isInstanceOf[CreateVolumes.Failure])
  }

  @Test
  def schedulerCreatesVolumes: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "volume1")
    )

    val o1 = offer("cpus:1; mem:2048; ports:1000; disk(test):10000")
    registry.scheduler.resourceOffers(schedulerDriver, Seq(o1))
    waitForEventLoop()

    assertEquals(DynamicVolumeState.Creating, broker.volumes.head.asInstanceOf[DynamicVolume].state)
  }

  @Test
  def launchBrokerWithVolume: Unit = {
    val broker = registry.cluster.addBroker(new Broker())
    broker.active = true
    broker.volumes = Seq(
      DynamicVolume(1000, "data0", "volume1", DynamicVolumeState.Creating)
    )

    val o1 = offer("cpus:1; mem:2048; ports:1000; disk(test)[volume1:test1]:10000")

    registry.scheduler.resourceOffers(schedulerDriver, Seq(o1))
    waitForEventLoop()

    assertEquals(DynamicVolumeState.Ready, broker.volumes.head.asInstanceOf[DynamicVolume].state)

  }
}
