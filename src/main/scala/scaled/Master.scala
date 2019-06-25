package scaled

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

import scaled.vnode.{ Master => VNodeMaster }
import scaled.coordinator.{ Master => CoordinatorMaster }
import scaled.coordinator.Coordinator

object Master {
  def props[Key, Command, State](spec: Spec[Key, Command, State]): Props = Props(new Master(spec))

  def command[Key, Command, Acc](master: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef): Unit =
    master.tell(CommandSingleKey(key, command, coordinator), sender)

  def coverageCommand[Key, Command, Acc](master: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef): Unit =
    master.tell(CommandSingleKey(key, command, coordinator), sender)

  final case class CommandSingleKey[Key, Command, Acc](key: Key, command: Command, coordinator: Coordinator[Acc])
  final case class CommandCoverage[Command, Acc](command: Command, coordinator: Coordinator[Acc])
}

class Master[Key, Command, State](spec: Spec[Key, Command, State]) extends AkkaActor with ActorLogging {
  import Master._

  override def preStart: Unit = {
    val vnodeMaster = this.context.actorOf(VNodeMaster.props(spec), "vnodeMaster")

    val coordinatorMaster = this.context.actorOf(CoordinatorMaster.props(vnodeMaster), "coordinatorMaster")

    this.context.become(this.initialized(vnodeMaster, coordinatorMaster))
  }

  private def initialized(vnodeMaster: ActorRef, coordinatorMaster: ActorRef): Receive = {
    case CommandSingleKey(key, command, coordinator) => {
      CoordinatorMaster.command(coordinatorMaster, key, command, coordinator)(this.sender)
    }

    case CommandCoverage(command, coordinator) => {
      CoordinatorMaster.coverageCommand(coordinatorMaster, command, coordinator)(this.sender)
    }
  }

  override def receive: Receive = AkkaActor.emptyBehavior
}
