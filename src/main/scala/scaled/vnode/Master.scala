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
  final case class LookupReply(vnodes: Seq[ActorRef])
}

class Master[Key, Command, State](builder: Builder[Command, State])(hashing: Hashing[Key]) extends AkkaActor with ActorLogging {
  import Master._

  val replicationFactor = builder.replicationFactor

  override def preStart: Unit = {
    val indexToVNode = (1 to Master.VNodeCount).map(i => i -> buildVNode(i)).toMap
    this.context.become(this.initialized(indexToVNode))
  }

  override def receive: Receive = AkkaActor.emptyBehavior

  private def initialized(indexToVNode: Map[Int, ActorRef]): Receive = {
    case l: Lookup[Key] => {
      val mainIndex: Int = consistentHash(l.key)
      val allIndices: Seq[Int] = getAllIndices(mainIndex)
      val vnodes = allIndices map indexToVNode

      this.sender ! LookupReply(vnodes)
    }
  }

  private def buildVNode(index: Int): ActorRef =
    this.context.actorOf(Actor.props(builder.build), f"vnode-${index}%04d")

  private def consistentHash(key: Key): Int = wrap(abs(hashing.hash(key)))

  private def wrap(index: Int): Int = index % Master.VNodeCount

  private def getAllIndices(mainIndex: Int): Seq[Int] =
    mainIndex.to(mainIndex + replicationFactor - 1).map(wrap)
}
