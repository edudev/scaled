package scaled.coordinator

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Master {
  def props(vnodeMaster: ActorRef): Props = Props(new Master(vnodeMaster))

  def command[Key, Command](master: ActorRef, key: Key, command: Command)(implicit sender: ActorRef): Unit =
    master.tell(CommandM(key, command), sender)

  final case class CommandM[Key, Command](key: Key, command: Command)
}

class Master(vnodeMaster: ActorRef) extends AkkaActor with ActorLogging {
  import Master._

  override def receive = {
    case CommandM(key, command) => {
      this.context.actorOf(Actor.props(this.sender, vnodeMaster, key, command))
    }
  }
}
