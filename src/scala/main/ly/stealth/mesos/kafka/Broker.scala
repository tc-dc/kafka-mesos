/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import java.util
import java.util.{Date, UUID}
import scala.collection.JavaConversions._
import ly.stealth.mesos.kafka.Broker.{Failover, Metrics, Stickiness}
import ly.stealth.mesos.kafka.json.JsonUtil
import net.elodina.mesos.util.{Constraint, Period, Range, Repr}

class Broker(_id: String = "0") {
  var id: String = _id
  @volatile var active: Boolean = false

  var cpus: Double = 1
  var mem: Long = 2048
  var heap: Long = 1024
  var port: Range = _
  var jmxPort: Range = _
  var volume: String = _
  var bindAddress: BindAddress = _
  var syslog: Boolean = false

  var constraints: Map[String, Constraint] = Map()
  var options: Map[String, String] = Map()
  var log4jOptions: Map[String, String] = Map()
  var jvmOptions: String = _

  var stickiness: Stickiness = new Stickiness()
  var failover: Failover = new Failover()

  var metrics: Metrics = Metrics()

  // broker has been modified while being in non stopped state, once stopped or before task launch becomes false
  var needsRestart: Boolean = false

  @volatile var task: Broker.Task = _
  @volatile var lastTask: Broker.Task = _

  /*
  An "steady state" broker is a broker that is either running happily,
  or stopped and won't be started.
   */
  def isSteadyState: Boolean = {
    (active && task != null && task.running) || !active
  }

  def shouldStart(hostname: String, now: Date = new Date()): Boolean =
    active && task == null

  def shouldStop: Boolean = !active && task != null && !task.stopping

  def registerStart(hostname: String): Unit = {
    stickiness.registerStart(hostname)
    failover.resetFailures()
  }

  def registerStop(now: Date = new Date(), failed: Boolean = false): Unit = {
    if (!failed || failover.failures == 0) stickiness.registerStop(now)

    if (failed) failover.registerFailure(now)
    else failover.resetFailures()
  }

  def state(now: Date = new Date()): String = {
    if (task != null && !task.starting) return task.state

    if (active) {
      if (failover.isWaitingDelay(now)) {
        var s = "failed " + failover.failures
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += " " + Repr.dateTime(failover.failureTime)
        s += ", next start " + Repr.dateTime(failover.delayExpires)
        return s
      }

      if (failover.failures > 0) {
        var s = "starting " + (failover.failures + 1)
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += ", failed " + Repr.dateTime(failover.failureTime)
        return s
      }

      return "starting"
    }

    "stopped"
  }

  def waitFor(state: String, timeout: Period, minDelay: Int = 100): Boolean = {
    def matches: Boolean = if (state != null) task != null && task.state == state else task == null

    var t = timeout.ms
    while (t > 0 && !matches) {
      val delay = Math.min(minDelay, t)
      Thread.sleep(delay)
      t -= delay
    }

    matches
  }

  def clone(newId: String): Broker = {
    val nb = new Broker(newId)
    nb.cpus = cpus
    nb.mem = mem
    nb.heap = heap
    nb.port = port
    nb.jmxPort = jmxPort
    nb.volume = volume
    nb.bindAddress = bindAddress
    nb.syslog = syslog
    nb.stickiness.period = stickiness.period
    nb.constraints = Map() ++ constraints
    nb.options = Map() ++ options
    nb.log4jOptions = Map() ++ log4jOptions
    nb.jvmOptions = jvmOptions
    nb.failover.delay = failover.delay
    nb.failover.maxDelay = failover.maxDelay
    nb.failover.maxTries = failover.maxTries

    nb
  }

  override def toString: String = {
    JsonUtil.toJson(this)
  }

}

object Broker {
  def nextTaskId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()

  def nextExecutorId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()

  def idFromTaskId(taskId: String): String = taskId.dropRight(37).replace(Config.frameworkName + "-", "")

  def idFromExecutorId(executorId: String): String = idFromTaskId(executorId)

  def isOptionOverridable(name: String): Boolean = !Set("broker.id", "port", "zookeeper.connect").contains(name)

  class Stickiness(_period: Period = new Period("10m")) {
    var period: Period = _period
    @volatile var hostname: String = null
    @volatile var stopTime: Date = null

    def expires: Date = if (stopTime != null) new Date(stopTime.getTime + period.ms) else null

    def registerStart(hostname: String): Unit = {
      this.hostname = hostname
      stopTime = null
    }

    def registerStop(now: Date = new Date()): Unit = {
      this.stopTime = now
    }

    def stickyTimeLeft(now: Date = new Date()): Int =
      if (stopTime == null || hostname == null)
        0
      else
        (((stopTime.getTime - now.getTime) + period.ms) / 1000).toInt

    def allowsHostname(hostname: String, now: Date = new Date()): Boolean =
      stickyTimeLeft(now) <= 0 || matchesHostname(hostname)

    def matchesHostname(hostname: String) =
      this.hostname == null || this.hostname == hostname

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Stickiness]) return false
      val other = obj.asInstanceOf[Stickiness]
      period == other.period && stopTime == other.stopTime && hostname == other.hostname
    }
  }

  class Failover(_delay: Period = new Period("1m"), _maxDelay: Period = new Period("10m")) {
    var delay: Period = _delay
    var maxDelay: Period = _maxDelay
    var maxTries: Integer = null

    @volatile var failures: Int = 0
    @volatile var failureTime: Date = null

    def currentDelay: Period = {
      if (failures == 0) return new Period("0")

      val multiplier = 1 << Math.min(30, failures - 1)
      val d = delay.ms * multiplier

      if (d > maxDelay.ms) maxDelay else new Period(delay.value * multiplier + delay.unit)
    }

    def delayExpires: Date = {
      if (failures == 0) return new Date(0)
      new Date(failureTime.getTime + currentDelay.ms)
    }

    def isWaitingDelay(now: Date = new Date()): Boolean = delayExpires.getTime > now.getTime

    def isMaxTriesExceeded: Boolean = {
      if (maxTries == null) return false
      failures >= maxTries
    }

    def registerFailure(now: Date = new Date()): Unit = {
      failures += 1
      failureTime = now
    }

    def resetFailures(): Unit = {
      failures = 0
      failureTime = null
    }

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Failover])
        return false
      val other = obj.asInstanceOf[Failover]

      (delay == other.delay
        && maxDelay == other.maxDelay
        && maxTries == other.maxTries
        && failures == other.failures
        && failureTime == other.failureTime)
    }
  }

  case class Task(
    id: String = null,
    slaveId: String = null,
    executorId: String = null,
    hostname: String = null,
    attributes: Map[String, String] = Map(),
    _state: String = State.STARTING
  ) {
    @volatile var state: String = _state
    var endpoint: Endpoint = null

    def starting: Boolean = state == State.STARTING

    def running: Boolean = state == State.RUNNING

    def stopping: Boolean = state == State.STOPPING

    def reconciling: Boolean = state == State.RECONCILING

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Task]) return false
      val other = obj.asInstanceOf[Task]
      (id == other.id
        && executorId == other.executorId
        && slaveId == other.slaveId
        && hostname == other.hostname
        && endpoint == other.endpoint
        && attributes == other.attributes)
    }
  }

  class Endpoint(s: String) {
    val (hostname: String, port: Int) = {
      val idx = s.indexOf(":")
      if (idx == -1) throw new IllegalArgumentException(s)

      (s.substring(0, idx), Integer.parseInt(s.substring(idx + 1)))
    }

    def this(hostname: String, port: Int) = this(hostname + ":" + port)

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Endpoint]) return false
      val endpoint = obj.asInstanceOf[Endpoint]
      hostname == endpoint.hostname && port == endpoint.port
    }

    override def hashCode(): Int = 31 * hostname.hashCode + port.hashCode()

    override def toString: String = hostname + ":" + port
  }

  case class Metrics(data: Map[String, Number] = Map(), timestamp: Long = 0) {
    def apply(metric: String) = data.get(metric)
  }

  object State {
    val STARTING = "starting"
    val RUNNING = "running"
    val RECONCILING = "reconciling"
    val STOPPING = "stopping"
  }

  type OtherAttributes = (String) => util.Collection[String]

  def NoAttributes: OtherAttributes = _ => Seq()
}
