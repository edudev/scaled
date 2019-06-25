package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

import scaled.vnode.{ Master => VNodeMaster, Actor => VNodeActor }

object Actor {
  def props[Key, Command, Acc](origin: ActorRef, vnodeMaster: ActorRef, key: Option[Key], command: Command, coordinator: Coordinator[Acc]): Props = Props(new Actor(origin, vnodeMaster, key, command, coordinator))

  final case class CoordinatorReply(reply: Any)
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
    vnodes.foreach(VNodeActor.command(_, command)(this.self))
    this.context.become(this.waiting(this.coordinator.init, Set(vnodes: _*)))
  }

  private def waiting(acc0: Acc, waitingAnswers0: Set[ActorRef]): Receive = {
    case VNodeActor.$CommandReply(reply) => {
      val vnode = this.sender()
      val waitingAnswers = waitingAnswers0 - vnode

      this.coordinator.accumulate(acc0, reply) match {
        case AccumulateReply(result) =>
          this.finish(result)
        case AccumulateContinue(acc) =>
          if (waitingAnswers.isEmpty)
            this.finish(this.coordinator.finish(acc))
          else
            this.context.become(this.waiting(acc, waitingAnswers))
      }
    }
  }

  private def finish(result: Any): Unit = {
    this.origin ! CoordinatorReply(result)
    this.context.stop(this.self)
  }
}
