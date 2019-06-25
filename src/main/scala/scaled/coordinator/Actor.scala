package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

import scaled.vnode.{ Master => VNodeMaster, Actor => VNodeActor }

object Actor {
  def props[Key, Command](origin: ActorRef, vnodeMaster: ActorRef, key: Key, command: Command): Props = Props(new Actor(origin, vnodeMaster, key, command))

  final case class CoordinatorReply(reply: Any)
}

class Actor[Key, Command](origin: ActorRef, vnodeMaster: ActorRef, key: Key, command: Command) extends AkkaActor with ActorLogging {
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
    this.context.become(this.waiting(vnodes))
  }

  private def waiting(vnodes: Seq[ActorRef]): Receive = {
    case VNodeActor.$CommandReply(reply) => {
      this.finish(reply)
    }
  }

  private def finish(reply: Any): Unit = {
    this.origin ! CoordinatorReply(reply)
    this.context.stop(this.self)
  }
}
