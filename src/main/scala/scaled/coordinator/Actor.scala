package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

import scaled.vnode.{ Master => VNodeMaster, Actor => VNodeActor }

object Actor {
  def props[Key, Command, Acc](origin: ActorRef, vnodeMaster: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc]): Props = Props(new Actor(origin, vnodeMaster, key, command, coordinator))

  final case class CoordinatorReply(reply: Any)
}

class Actor[Key, Command, Acc](origin: ActorRef, vnodeMaster: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc]) extends AkkaActor with ActorLogging {
  import Actor._

  override def preStart: Unit = {
    this.prepare
  }

  private def prepare: Unit = {
    VNodeMaster.lookup(vnodeMaster, key)(this.self)
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
      val acc = this.coordinator.accumulate(acc0, reply)

      if (waitingAnswers.isEmpty)
        this.finish(acc)
      else
        this.context.become(this.waiting(acc, waitingAnswers))
    }
  }

  private def finish(acc: Acc): Unit = {
    val reply = this.coordinator.finish(acc)
    this.origin ! CoordinatorReply(reply)
    this.context.stop(this.self)
  }
}
