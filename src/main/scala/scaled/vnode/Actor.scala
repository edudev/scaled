package scaled.vnode

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Actor {
  def props[Command, State](vnode: VNode[Command, State]): Props = Props(new Actor(vnode))

  final case class $Command[Command](command: Command)
  final case class $CommandReply(reply: Any)

  def command[Command](vnode: ActorRef, command: Command)(implicit sender: ActorRef) = vnode ! $Command(command)
}

class Actor[Command, State](vnode: VNode[Command, State]) extends AkkaActor with ActorLogging {
  import Actor._

  var state: State = _

  override def preStart: Unit = {
    this.state = this.vnode.init
  }

  override def postStop: Unit = {
    this.vnode.terminate(this.state)
  }

  override def receive = {
    // abstract type pattern Command is unchecked since it is eliminated by erasure
    //case $Command(command: Command) => {

    case c: $Command[Command] => {
      val command = c.command
      val sender = Sender(this.sender, this.self)

      this.vnode.handle_command(sender, command, this.state) match {
        case CommandReply(reply, newState) => {
          this.state = newState
          sender.send(reply)
        }
        case CommandNoReply(newState) => {
          this.state = newState
        }
      }
    }
  }
}
