package scaled.vnode

import akka.actor.ActorRef

case class Sender(private val ref: ActorRef, private val vnode: ActorRef) {
  def send(reply: Any) = this.ref.tell(Actor.$CommandReply(reply), vnode)
}

sealed trait CommandResponse[State]
case class CommandReply[State](reply: Any, state: State) extends CommandResponse[State]
case class CommandNoReply[State](state: State) extends CommandResponse[State]
