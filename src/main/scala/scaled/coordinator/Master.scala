package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Master {
  def props(vnodeMaster: ActorRef): Props = Props(new Master(vnodeMaster))

  def command[Key, Command, Acc](master: ActorRef, key: Key, command: Command, coordinator: Coordinator[Acc])(implicit sender: ActorRef): Unit =
    master.tell(CommandM(key, command, coordinator), sender)

  final case class CommandM[Key, Command, Acc](key: Key, command: Command, coordinator: Coordinator[Acc])
}

class Master(vnodeMaster: ActorRef) extends AkkaActor with ActorLogging {
  import Master._

  override def receive = {
    case CommandM(key, command, coordinator) => {
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, key, command, coordinator))
    }
  }
}
