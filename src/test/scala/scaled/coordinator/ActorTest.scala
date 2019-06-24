package scaled.coordinator

import scala.util.hashing.MurmurHash3
import scala.concurrent.duration._

import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, WordSpecLike, Matchers }
import akka.actor.{ ActorSystem, ActorRef, PoisonPill }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import akka.pattern.ask

import scaled.vnode.{ Master => VNodeMaster }
import scaled.vnode.{ Actor => VNodeActor }

import scaled.samples.CounterVNode

class ActorSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with WordSpecLike
  with BeforeAndAfter
  with BeforeAndAfterAll {

  def this() = {
    this(ActorSystem("ActorSpec"))
  }

  override def afterAll: Unit = {
    shutdown(system)
  }

  var vnodeMaster: ActorRef = _

  before {

    vnodeMaster = system.actorOf(VNodeMaster.props(CounterVNode.builder)(MurmurHash3.stringHashing))
  }

  after {
    vnodeMaster ! PoisonPill
  }

  import Actor.CoordinatorReply

  "A VNode Coordinator" should {
    import VNodeMaster.LookupReply
    import VNodeActor.$CommandReply
    import CounterVNode._

    "forward commands" in {
      val probe = TestProbe()

      VNodeMaster.lookup(vnodeMaster, "key 1")(probe.ref)
      val key1vnode = probe.expectMsgType[LookupReply].vnode

      VNodeMaster.lookup(vnodeMaster, "key 2")(probe.ref)
      val key2vnode = probe.expectMsgType[LookupReply].vnode

      VNodeActor.command(key1vnode, Get)(probe.ref)
      probe.expectMsg($CommandReply(0))

      VNodeActor.command(key2vnode, Get)(probe.ref)
      probe.expectMsg($CommandReply(0))

      VNodeActor.command(key1vnode, Set(130))(probe.ref)
      probe.expectMsg($CommandReply(0))

      val coordinator = system.actorOf(Actor.props(probe.ref, vnodeMaster, "key 1", Get))

      probe.expectMsg(CoordinatorReply(130))

      VNodeActor.command(key1vnode, Get)(probe.ref)
      probe.expectMsg($CommandReply(130))

      VNodeActor.command(key2vnode, Get)(probe.ref)
      probe.expectMsg($CommandReply(0))
    }
  }
}

