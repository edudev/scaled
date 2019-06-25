package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }
import akka.actor.Terminated

import scaled.vnode.{ Master => VNodeMaster, Actor => VNodeActor }

object Actor {
  def props[Key, Command, Acc](origin: ActorRef, vnodeMaster: ActorRef, key: Option[Key], command: Command, coordinator: Coordinator[Acc]): Props = Props(new Actor(origin, vnodeMaster, key, command, coordinator))

  sealed trait CoordinatorResponse
  final case class CoordinatorReply(reply: Any) extends CoordinatorResponse
  final case object CoordinatorNoReply extends CoordinatorResponse
}

class Actor[Key, Command, Acc](origin: ActorRef, vnodeMaster: ActorRef, key: Option[Key], command: Command, coordinator: Coordinator[Acc]) extends AkkaActor with ActorLogging {
  import Actor._

  override def preStart: Unit = {
    this.prepare
  }

  private def prepare: Unit = {
    key match {
      case Some(k) => VNodeMaster.lookup(vnodeMaster, k)(this.self)
      case None => VNodeMaster.lookupAll(vnodeMaster)(this.self)
    }
  }

  override def receive = {
    case VNodeMaster.LookupReply(vnodes: Seq[ActorRef]) => {
      this.execute(vnodes)
    }
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
          this.finish(result)
        case AccumulateContinue(acc) => {
          this.shouldContinueWait(waitingAnswers0, vnode, acc)
        }
      }
    }
    case Terminated(vnode) => {
      this.shouldContinueWait(waitingAnswers0, vnode, acc0)
    }
  }

  private def mapReply(reply: Option[Any]): CoordinatorResponse =
    reply.fold(CoordinatorNoReply: CoordinatorResponse)(CoordinatorReply)

  private def finish(result: Option[Any]): Unit = {
    this.origin ! mapReply(result)
    this.context.stop(this.self)
  }

  private def shouldContinueWait(waitingAnswers0: Set[ActorRef], vnode: ActorRef, acc: Acc) = {
    this.context.unwatch(vnode)
    val waitingAnswers = waitingAnswers0 - vnode

    if (waitingAnswers.isEmpty) {
      this.finish(this.coordinator.finish(acc))
    }
    else
      this.context.become(this.waiting(acc, waitingAnswers))
  }
}
