package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Master {
  def props(vnodeMaster: ActorRef): Props = Props(new Master(vnodeMaster))

  def command[Key, Command, Acc](master: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef): Unit =
    master.tell(CommandSingleKey(key, command, coordinator), sender)

  def coverageCommand[Command, Acc](master: ActorRef, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef): Unit =
    master.tell(CommandCoverage(command, coordinator), sender)

  final case class CommandSingleKey[Key, Command, Acc](key: Key, command: Command, coordinator: Coordinator[Acc])
  final case class CommandCoverage[Command, Acc](command: Command, coordinator: Coordinator[Acc])
}

class Master(vnodeMaster: ActorRef) extends AkkaActor with ActorLogging {
  import Master._

  override def receive = {
    case CommandSingleKey(key, command, coordinator) =>
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, Some(key), command, coordinator))

    case CommandCoverage(command, coordinator) =>
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, None, command, coordinator))
  }
}
