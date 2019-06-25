package scaled.samples

import scala.util.hashing.MurmurHash3

import scaled.vnode.VNode
import scaled.vnode.Sender
import scaled.vnode.CommandReply
import scaled.vnode.CommandNoReply

import scaled.Spec

object DummyVNode {
  val spec = new Spec[String, Nothing, Unit] {
    def build = new DummyVNode
    val replicationFactor: Int = 3
    val hashing = MurmurHash3.stringHashing
  }
}

class DummyVNode extends VNode[Nothing, Unit] {
  def init = ()
  def handleCommand(sender: Sender, command: Nothing, counter: Unit) = ???
}
