package scaled.coordinator

import scala.concurrent.duration.FiniteDuration

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }
import akka.actor.Terminated

import scaled.vnode.{ Master => VNodeMaster, Actor => VNodeActor }

object Actor {
  def props[Key, Command, Acc](
    origin: ActorRef,
    vnodeMaster: ActorRef,
    key: Option[Key],
    command: Command,
    coordinator: Coordinator[Acc],
    timeout: FiniteDuration): Props =
      Props(new Actor(origin, vnodeMaster, key, command, coordinator, timeout))

  final case object CoordinatorTimeout

  sealed trait CoordinatorResponse
  final case class CoordinatorReply(reply: Any) extends CoordinatorResponse
  final case object CoordinatorNoReply extends CoordinatorResponse
}

class Actor[Key, Command, Acc](
  origin: ActorRef,
  vnodeMaster: ActorRef,
  key: Option[Key],
  command: Command,
  coordinator: Coordinator[Acc],
  timeout: FiniteDuration) extends AkkaActor with ActorLogging {

  import Actor._

  override def preStart: Unit = {
    this.prepare
  }

  private def prepare: Unit = {
    import context.dispatcher

    this.context.system.scheduler.scheduleOnce(timeout, self, CoordinatorTimeout)

    key match {
      case Some(k) => VNodeMaster.lookup(vnodeMaster, k)(this.self)
      case None => VNodeMaster.lookupAll(vnodeMaster)(this.self)
    }
  }

  override def receive = {
    case VNodeMaster.LookupReply(vnodes: Seq[ActorRef]) =>
      this.execute(vnodes)
    case CoordinatorTimeout =>
      this.finish(this.coordinator.init)
  }

  private def execute(vnodes: Seq[ActorRef]): Unit = {
    vnodes.foreach(this.context.watch(_))
    vnodes.foreach(VNodeActor.command(_, command)(this.self))
    this.context.become(this.waiting(this.coordinator.init, Set(vnodes: _*)))
  }

  private def waiting(acc0: Acc, waitingAnswers0: Set[ActorRef]): Receive = {
    case VNodeActor.$CommandReply(reply) => {
      val vnode = this.sender()

      this.coordinator.accumulate(acc0, reply) match {
        case AccumulateReply(result) =>
          this.sendAndStop(result)
        case AccumulateContinue(acc) => {
          this.shouldContinueWait(waitingAnswers0, vnode, acc)
        }
      }
    }

    case Terminated(vnode) =>
      this.shouldContinueWait(waitingAnswers0, vnode, acc0)

    case CoordinatorTimeout =>
      this.finish(acc0)
  }

  private def mapReply(reply: Option[Any]): CoordinatorResponse =
    reply.fold(CoordinatorNoReply: CoordinatorResponse)(CoordinatorReply)

  private def finish(acc: Acc): Unit = {
    this.sendAndStop(this.coordinator.finish(acc))
  }

  private def sendAndStop(result: Option[Any]): Unit = {
    this.origin ! mapReply(result)
    this.context.stop(this.self)
  }

  private def shouldContinueWait(waitingAnswers0: Set[ActorRef], vnode: ActorRef, acc: Acc) = {
    this.context.unwatch(vnode)
    val waitingAnswers = waitingAnswers0 - vnode

    if (waitingAnswers.isEmpty) {
      this.finish(acc)
    }
    else
      this.context.become(this.waiting(acc, waitingAnswers))
  }
}
