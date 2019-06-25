package scaled.coordinator

import scala.concurrent.duration.FiniteDuration

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Master {
  def props(vnodeMaster: ActorRef): Props = Props(new Master(vnodeMaster))

  def command[Key, Command, Acc](master: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef, timeout: FiniteDuration): Unit =
    master.tell(CommandSingleKey(key, command, coordinator, timeout), sender)

  def coverageCommand[Command, Acc](master: ActorRef, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef, timeout: FiniteDuration): Unit =
    master.tell(CommandCoverage(command, coordinator, timeout), sender)

  final case class CommandSingleKey[Key, Command, Acc](key: Key, command: Command, coordinator: Coordinator[Acc], timeout: FiniteDuration)
  final case class CommandCoverage[Command, Acc](command: Command, coordinator: Coordinator[Acc], timeout: FiniteDuration)
}

class Master(vnodeMaster: ActorRef) extends AkkaActor with ActorLogging {
  import Master._

  override def receive = {
    case CommandSingleKey(key, command, coordinator, timeout) =>
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, Some(key), command, coordinator, timeout))

    case CommandCoverage(command, coordinator, timeout) =>
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, None, command, coordinator, timeout))
  }
}
