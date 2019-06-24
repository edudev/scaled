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
    case VNodeMaster.LookupReply(vnode: ActorRef) => {
      this.execute(vnode)
    }
  }

  private def execute(vnode: ActorRef): Unit = {
    VNodeActor.command(vnode, command)(this.self)
    this.context.become(this.waiting(vnode))
  }

  private def waiting(vnode: ActorRef): Receive = {
    case VNodeActor.$CommandReply(reply) => {
      this.finish(reply)
    }
  }

  private def finish(reply: Any): Unit = {
    this.origin ! CoordinatorReply(reply)
  }
}
