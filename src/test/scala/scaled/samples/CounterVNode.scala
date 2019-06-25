package scaled.samples

import scaled.vnode.VNode
import scaled.vnode.Builder
import scaled.vnode.Sender
import scaled.vnode.CommandReply
import scaled.vnode.CommandNoReply


object CounterVNode {
  sealed trait Command
  case object Get extends Command
  case object Increment extends Command
  case object Clear extends Command
  case class Set(value: Int) extends Command

  val builder = new Builder[Command, Int] {
    def build = new CounterVNode
    val replicationFactor: Int = 3
  }
}

import CounterVNode._

class CounterVNode extends VNode[Command, Int] {
  def init = 0
  def handle_command(sender: Sender, command: Command, counter: Int) =
    command match {
      case Get => CommandReply(counter, counter)
      case Increment => CommandReply(counter, counter + 1)
      case Clear => CommandNoReply(this.init)
      case Set(value) => {
        sender.send(counter)
        CommandNoReply(value)
      }
    }
}
