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

package ly.stealth.mesos.kafka

import java.io.{File, FileInputStream}
import java.util.Properties
import java.net.URI
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util.{Period, Version}

object SchedulerVersion {
  val value = "0.10.1.0-SNAPSHOT"
}

trait ConfigComponent {
  def config: Config
}

case class Config(
  debug: Boolean = false,
  storage: String = "file:kafka-mesos.json",

  master: String = null,
  principal: String = null,
  secret: String = null,
  user: String = null,

  frameworkName: String = "kafka",
  frameworkRole: String = "*",
  frameworkTimeout: Period = new Period("30d"),

  reconciliationTimeout: Period = new Period("5m"),
  reconciliationAttempts: Int = 6,
  reconciliationInterval: Period = new Period("30m"),

  jre: File = null,
  log: File = null,
  api: String = null,
  bindAddress: BindAddress = null,
  zk: String = null
) {
  def apiPort: Int = {
    val port = new URI(api).getPort
    if (port == -1) 80 else port
  }
}

object Config {
  val DEFAULT_FILE = new File("kafka-mesos.properties")
  def get = theConfig
  def set(c: Config) = theConfig = c

  private[this] var theConfig: Config = Config()

  def debug: Boolean = theConfig.debug
  def storage: String = theConfig.storage

  def master: String = theConfig.master
  def principal: String = theConfig.principal
  def secret: String = theConfig.secret
  def user: String = theConfig.user

  def frameworkName: String = theConfig.frameworkName
  def frameworkRole: String = theConfig.frameworkRole
  def frameworkTimeout: Period = theConfig.frameworkTimeout

  def reconciliationTimeout: Period = theConfig.reconciliationTimeout
  def reconciliationAttempts: Int = theConfig.reconciliationAttempts
  def reconciliationInterval: Period = theConfig.reconciliationInterval

  def jre: File = theConfig.jre
  def log: File = theConfig.log
  def api: String = theConfig.api
  def bindAddress: BindAddress = theConfig.bindAddress
  def zk: String = theConfig.zk

  def apiPort: Int = theConfig.apiPort

  def replaceApiPort(port: Int): Unit = {
    val prev: URI = new URI(api)
    theConfig = theConfig.copy(
      api = "" + new URI(
        prev.getScheme, prev.getUserInfo,
        prev.getHost, port,
        prev.getPath, prev.getQuery, prev.getFragment
      ))
  }

  private[kafka] def load(file: File): Unit = {
    val props: Properties = new Properties()
    val stream: FileInputStream = new FileInputStream(file)

    props.load(stream)
    stream.close()

    var config = Config()

    if (props.containsKey("debug")) config = config.copy(debug = java.lang.Boolean.valueOf(props.getProperty("debug")))
    if (props.containsKey("storage")) config = config.copy(storage = props.getProperty("storage"))

    if (props.containsKey("master")) config = config.copy(master = props.getProperty("master"))
    if (props.containsKey("user")) config = config.copy(user = props.getProperty("user"))
    if (props.containsKey("principal")) config = config.copy(principal = props.getProperty("principal"))
    if (props.containsKey("secret")) config = config.copy(secret = props.getProperty("secret"))

    if (props.containsKey("framework-name")) config = config.copy(frameworkName = props.getProperty("framework-name"))
    if (props.containsKey("framework-role")) config = config.copy(frameworkRole = props.getProperty("framework-role"))
    if (props.containsKey("framework-timeout")) config = config.copy(frameworkTimeout = new Period(props.getProperty("framework-timeout")))

    if (props.containsKey("reconciliation-timeout")) config = config.copy(reconciliationTimeout = new Period(props.getProperty("reconciliation-timeout")))
    if (props.containsKey("reconciliation-attempts")) config = config.copy(reconciliationAttempts = Integer.valueOf("reconciliation-attempts"))
    if (props.containsKey("reconciliation-interval")) config = config.copy(reconciliationInterval = new Period(props.getProperty("reconciliation-interval")))

    if (props.containsKey("jre")) config = config.copy(jre = new File(props.getProperty("jre")))
    if (props.containsKey("log")) config = config.copy(log = new File(props.getProperty("log")))
    if (props.containsKey("api")) config = config.copy(api = props.getProperty("api"))
    if (props.containsKey("bind-address")) config = config.copy(bindAddress = new BindAddress(props.getProperty("bind-address")))
    if (props.containsKey("zk")) config = config.copy(zk = props.getProperty("zk"))

    theConfig = config
  }

  override def toString: String = {
    s"""
      |debug: $debug, storage: $storage
      |mesos: master=$master, user=${if (user == null || user.isEmpty) "<default>" else user}, principal=${if (principal != null) principal else "<none>"}, secret=${if (secret != null) "*****" else "<none>"}
      |framework: name=$frameworkName, role=$frameworkRole, timeout=$frameworkTimeout
      |api: $api, bind-address: ${if (bindAddress != null) bindAddress else "<all>"}, zk: $zk, jre: ${if (jre == null) "<none>" else jre}
    """.stripMargin.trim
  }
}
