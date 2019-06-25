package scaled.vnode

import scala.util.hashing.MurmurHash3
import scala.concurrent.duration._

import org.scalatest.{ BeforeAndAfterAll, WordSpecLike, Matchers }
import akka.actor.{ ActorSystem, Identify, ActorIdentity }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import akka.pattern.ask

import scaled.samples.DummyVNode

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
      val master = system.actorOf(Master.props(DummyVNode.builder)(MurmurHash3.stringHashing))

      val probe = TestProbe()
      system.actorSelection(master.path / "*").tell(Identify(420), probe.ref)

      val identities = (1 to Master.VNodeCount).map(_ => probe.expectMsgType[ActorIdentity])

      identities.size shouldBe Master.VNodeCount

      probe.expectNoMsg(100.millis)
    }

    "lookup children" in {
      import Master._

      val master = system.actorOf(Master.props(DummyVNode.builder)(MurmurHash3.stringHashing))

      val probe = TestProbe()

      Master.lookup(master, "key 1")(probe.ref)
      val key1vnode = probe.expectMsgType[LookupReply]

      Master.lookup(master, "key 2")(probe.ref)
      val key2vnode = probe.expectMsgType[LookupReply]

      key1vnode.vnodes === key2vnode.vnodes shouldBe false
    }
  }
}
