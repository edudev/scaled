package scaled

import scala.util.hashing.Hashing

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

import scaled.vnode.{ Master => VNodeMaster }
import scaled.vnode.Builder
import scaled.coordinator.{ Master => CoordinatorMaster }

object Master {
  def props[Key, Command, State](builder: Builder[Command, State])(implicit hashing: Hashing[Key]): Props = Props(new Master(builder)(hashing))

  def command[Key, Command](master: ActorRef, key: Key, command: Command)(implicit sender: ActorRef): Unit =
    master.tell(CommandM(key, command), sender)

  final case class CommandM[Key, Command](key: Key, command: Command)
}

class Master[Key, Command, State](builder: Builder[Command, State])(hashing: Hashing[Key]) extends AkkaActor with ActorLogging {
  import Master._

  override def preStart: Unit = {
    val vnodeMaster = this.context.actorOf(VNodeMaster.props(builder)(hashing), "vnodeMaster")

    val coordinatorMaster = this.context.actorOf(CoordinatorMaster.props(vnodeMaster), "coordinatorMaster")

    this.context.become(this.initialized(vnodeMaster, coordinatorMaster))
  }

  private def initialized(vnodeMaster: ActorRef, coordinatorMaster: ActorRef): Receive = {
    case CommandM(key, command) => {
      CoordinatorMaster.command(coordinatorMaster, key, command)(this.sender)
    }
  }

  override def receive: Receive = AkkaActor.emptyBehavior
}
