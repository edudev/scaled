package scaled.samples

import scala.util.hashing.MurmurHash3

import scaled.vnode.VNode
import scaled.vnode.Sender
import scaled.vnode.CommandReply
import scaled.vnode.CommandNoReply

import scaled.Spec

import scaled.coordinator.Coordinator


object CounterVNode {
  sealed trait Command
  case object Get extends Command
  case object Increment extends Command
  case object Clear extends Command
  case class Set(value: Int) extends Command

  val spec = new Spec[String, Command, Int] {
    val replicationFactor: Int = 3
    def build = new CounterVNode
    val hashing = MurmurHash3.stringHashing
  }
}

import CounterVNode._

class CounterVNode extends VNode[Command, Int] {
  def init = 0
  def handleCommand(sender: Sender, command: Command, counter: Int) =
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
