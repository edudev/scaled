package scaled.vnode

import akka.actor.ActorRef

case class Sender(private val ref: ActorRef) {
  def send(reply: Any) = this.ref ! Actor.$CommandReply(reply)
}

sealed trait CommandResponse[State]
case class CommandReply[State](reply: Any, state: State) extends CommandResponse[State]
case class CommandNoReply[State](state: State) extends CommandResponse[State]
