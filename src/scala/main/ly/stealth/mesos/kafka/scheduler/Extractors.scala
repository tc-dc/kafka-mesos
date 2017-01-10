package ly.stealth.mesos.kafka.scheduler.mesos

import ly.stealth.mesos.kafka.Broker
import org.apache.mesos.Protos.{TaskState, TaskStatus}


abstract class TaskStatusExtractor(states: Set[TaskState]) {
  def this(state: TaskState) = this(Set(state))

  def unapply(status: TaskStatus) =
    if (states.contains(status.getState)) Some(status) else None
}

object TaskStarting extends TaskStatusExtractor(Set(TaskState.TASK_STARTING, TaskState.TASK_STAGING)) {}
object TaskRunning extends TaskStatusExtractor(TaskState.TASK_RUNNING) {}
object TaskKilling extends TaskStatusExtractor(TaskState.TASK_KILLING) {}
object TaskLost extends TaskStatusExtractor(TaskState.TASK_LOST) {}
object TaskExited extends TaskStatusExtractor(Set(
  TaskState.TASK_ERROR, TaskState.TASK_FAILED, TaskState.TASK_FINISHED,
  TaskState.TASK_KILLED, TaskState.TASK_LOST
)) {}

object Reconciling {
  def unapply(tup: (Option[Broker], TaskStatus)) = tup match {
    case (ReconcilingBroker(broker), status)
      if status.getReason == TaskStatus.Reason.REASON_RECONCILIATION =>
      Some((broker, status))
    case _ => None
  }
}


abstract class BrokerTaskTest(test: (Broker.Task => Boolean)) {
  def unapply(maybeBroker: Option[Broker]) = maybeBroker match {
    case b@Some(broker) if broker.task != null && test(broker.task) => b
    case _ => None
  }
}

object StartingBroker extends BrokerTaskTest(_.starting) {}
object RunningBroker extends BrokerTaskTest(_.running) {}
object StoppingBroker extends BrokerTaskTest(_.stopping) {}
object ReconcilingBroker extends BrokerTaskTest(_.reconciling) {}
object StoppedBroker {
  def unapply(maybeBroker: Option[Broker]) = maybeBroker match {
    case b@Some(broker) if broker.task == null => b
    case _ => None
  }
}
object HasTask {
  def unapply(maybeBroker: Option[Broker]) = maybeBroker match {
    case b@Some(broker) if broker.task != null => b
    case _ => None
  }
}
