package scaled.vnode

import scala.concurrent.duration._

import org.scalatest.{ BeforeAndAfterAll, WordSpecLike, Matchers }
import akka.actor.{ ActorSystem, PoisonPill }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import akka.pattern.ask


object CounterVNode {
  sealed trait Command
  case object Get extends Command
  case object Increment extends Command
  case object Clear extends Command
  case class Set(value: Int) extends Command

  def apply: CounterVNode = new CounterVNode
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

class ActorSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with WordSpecLike
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("ActorSpec"))

  override def afterAll: Unit = {
    shutdown(system)
  }

  "A VNode Actor" should {
    "call init" in {
      val probe = TestProbe()

      val vnode = system.actorOf(Actor.props(new CounterVNode() {
        override def init = {
          probe.ref ! "init called"
          super.init
        }
      }))

      probe.expectMsg("init called")
    }

    "call terminate" in {
      val probe = TestProbe()

      val vnode = system.actorOf(Actor.props(new CounterVNode() {
        override def init = 140
        override def terminate(counter: Int) = {
          probe.ref ! ("terminate called", counter)
          super.terminate(counter)
        }
      }))

      vnode ! PoisonPill

      probe.expectMsg(("terminate called", 140))
    }

    "be able to reply" in {
      val vnode = system.actorOf(Actor.props(CounterVNode.apply))
      val probe = TestProbe()

      Actor.command(vnode, Get)(probe.ref)
      probe.expectMsg(0)
    }

    "be able to change its state and reply" in {
      val vnode = system.actorOf(Actor.props(CounterVNode.apply))
      val probe = TestProbe()

      Actor.command(vnode, Increment)(probe.ref)
      probe.expectMsg(0)
      Actor.command(vnode, Get)(probe.ref)
      probe.expectMsg(1)
    }

    "be able to change its state without replying" in {
      val vnode = system.actorOf(Actor.props(CounterVNode.apply))
      val probe = TestProbe()

      Actor.command(vnode, Increment)(probe.ref)
      probe.expectMsg(0)

      Actor.command(vnode, Clear)(probe.ref)
      probe.expectNoMsg(100.millis)

      Actor.command(vnode, Get)(probe.ref)
      probe.expectMsg(0)
    }

    "be able to change its state and reply, separetely" in {
      val vnode = system.actorOf(Actor.props(CounterVNode.apply))
      val probe = TestProbe()

      Actor.command(vnode, Set(10))(probe.ref)
      probe.expectMsg(0)
      Actor.command(vnode, Get)(probe.ref)
      probe.expectMsg(10)
    }
  }
}
