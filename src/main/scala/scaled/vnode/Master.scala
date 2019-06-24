package scaled.vnode

import Math.abs
import scala.util.hashing.Hashing

import akka.actor.{ Actor => AkkaActor, ActorRef, Props, ActorLogging }

object Master {
  val VNodeCount: Int = 64

  def props[Key, Command, State](builder: Builder[Command, State])(implicit hashing: Hashing[Key]): Props = Props(new Master(builder)(hashing))

  def lookup[Key](master: ActorRef, key: Key)(implicit sender: ActorRef): Unit =
    master.tell(Lookup(key), sender)

  final case class Lookup[Key](key: Key)
  final case class LookupReply(vnode: ActorRef)
}

class Master[Key, Command, State](builder: Builder[Command, State])(hashing: Hashing[Key]) extends AkkaActor with ActorLogging {
  import Master._

  var indexToVNode: Map[Int, ActorRef] = _

  override def preStart: Unit = {
    this.indexToVNode = (1 to Master.VNodeCount).map(i => i -> buildVNode(i)).toMap
  }

  override def receive = {
    case l: Lookup[Key] => {
      val index: Int = consistentHash(l.key)
      val vnode = indexToVNode(index)

      this.sender ! LookupReply(vnode)
    }
  }

  private def buildVNode(index: Int): ActorRef =
    this.context.actorOf(Actor.props(builder.build), f"vnode-${index}%04d")

  private def consistentHash(key: Key): Int = abs(hashing.hash(key)) % Master.VNodeCount
}
