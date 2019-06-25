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

  // needed only for the terminate callback...
  var state: State = _

  override def preStart: Unit = {
    val state = this.vnode.init
    this.context.become(this.setState(state))
  }

  override def postStop: Unit = {
    this.vnode.terminate(this.state)
  }

  override def receive: Receive = AkkaActor.emptyBehavior

  private def setState(state: State): Receive = {
    this.state = state

    {
      // abstract type pattern Command is unchecked since it is eliminated by erasure
      //case $Command(command: Command) => {

      case c: $Command[Command] =>
        val newState = this.doHandleCommand(Sender(this.sender, this.self), c.command, state)
        this.context.become(this.setState(newState))
    }
  }

  private def doHandleCommand(sender: Sender, command: Command, state: State): State =
    this.vnode.handleCommand(sender, command, state) match {
      case CommandReply(reply, newState) => {
        sender.send(reply)
        newState
      }

      case CommandNoReply(newState) =>
        newState
    }
}
