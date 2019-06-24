package scaled.vnode

import scala.util.hashing.MurmurHash3
import scala.concurrent.duration._

import org.scalatest.{ BeforeAndAfterAll, WordSpecLike, Matchers }
import akka.actor.{ ActorSystem, Identify, ActorIdentity }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import akka.pattern.ask


object DummyVNode {
  def apply: DummyVNode = new DummyVNode
}

import DummyVNode._

class DummyVNode extends VNode[Nothing, Unit] {
  def init = ()
  def handle_command(sender: Sender, command: Nothing, counter: Unit) = ???
}

class MasterSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with WordSpecLike
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("MasterSpec"))

  override def afterAll: Unit = {
    shutdown(system)
  }

  "A VNode Master" should {
    "start children" in {
      val builder: Builder[Nothing, Unit] = new Builder[Nothing, Unit] {
        def build = DummyVNode.apply
      }

      val master = system.actorOf(Master.props(builder)(MurmurHash3.stringHashing))

      val probe = TestProbe()
      system.actorSelection(master.path / "*").tell(Identify(420), probe.ref)

      val identities = (1 to Master.VNodeCount).map(_ => probe.expectMsgType[ActorIdentity])

      identities.size shouldBe Master.VNodeCount

      probe.expectNoMsg(100.millis)
    }

    "lookup children" in {
      import Master._

      val builder: Builder[Nothing, Unit] = new Builder[Nothing, Unit] {
        def build = DummyVNode.apply
      }

      val master = system.actorOf(Master.props(builder)(MurmurHash3.stringHashing))

      val probe = TestProbe()

      master.tell(Lookup("key 1"), probe.ref)
      val key1vnode = probe.expectMsgType[LookupResult]

      master.tell(Lookup("key 2"), probe.ref)
      val key2vnode = probe.expectMsgType[LookupResult]

      key1vnode === key2vnode shouldBe false
    }
  }
}
