package scaled.samples

import scaled.vnode.VNode
import scaled.vnode.Builder
import scaled.vnode.Sender
import scaled.vnode.CommandReply
import scaled.vnode.CommandNoReply

object DummyVNode {
  val builder = new Builder[Nothing, Unit] {
    def build = new DummyVNode
    val replicationFactor: Int = 3
  }
}

class DummyVNode extends VNode[Nothing, Unit] {
  def init = ()
  def handleCommand(sender: Sender, command: Nothing, counter: Unit) = ???
}
