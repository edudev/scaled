package scaled.samples

import scala.util.hashing.MurmurHash3

import scaled.vnode.VNode
import scaled.vnode.Sender
import scaled.vnode.CommandReply
import scaled.vnode.CommandNoReply

import scaled.Spec

import scaled.coordinator.Coordinator


object KvVNode {
  sealed trait Command
  case class Get(key: String) extends Command
  case class Put[V](key: String, value: V) extends Command
  case object List extends Command

  def spec[V] = new Spec[String, Command, Map[String, V]] {
    val replicationFactor: Int = 3
    def build = new KvVNode[V]
    val hashing = MurmurHash3.stringHashing
  }
}

import KvVNode._

class KvVNode[V] extends VNode[Command, Map[String, V]] {
  def init = Map.empty[String, V]

  def handleCommand(sender: Sender, command: Command, state0: Map[String, V]) =
    command match {
      case Get(key) => CommandReply(get(state0, key), state0)
      case put: Put[V] => {
        val oldValue = get(state0, put.key)
        CommandReply(oldValue, state0.updated(put.key, put.value))
      }
      case List => CommandReply(state0.keySet, state0)
    }

  private def get(state0: Map[String, V], key: String): Option[V] =
    state0.get(key)
}
