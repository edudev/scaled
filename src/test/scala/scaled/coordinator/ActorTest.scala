package scaled.coordinator

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

    vnodeMaster = system.actorOf(VNodeMaster.props(CounterVNode.spec))
  }

  after {
    vnodeMaster ! PoisonPill
  }

  val majorityCoordinator: Coordinator[List[Any]] = new MajorityCoordinator(CounterVNode.spec.replicationFactor)

  import Actor.CoordinatorReply
  import Actor.CoordinatorNoReply

  "A VNode Coordinator" should {
    import VNodeMaster.LookupReply
    import VNodeMaster.Lookup
    import VNodeActor.$CommandReply
    import CounterVNode._

    "forward commands" in {
      val probe = TestProbe()

      VNodeMaster.lookup(vnodeMaster, "key 1")(probe.ref)
      val key1vnodes = probe.expectMsgType[LookupReply].vnodes

      VNodeMaster.lookup(vnodeMaster, "key 2")(probe.ref)
      val key2vnodes = probe.expectMsgType[LookupReply].vnodes

      key1vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Get)(probe.ref)
        probe.expectMsg($CommandReply(0))
      })

      key2vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Get)(probe.ref)
        probe.expectMsg($CommandReply(0))
      })

      key1vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Set(130))(probe.ref)
        probe.expectMsg($CommandReply(0))
      })

      val coordinator = system.actorOf(Actor.props(probe.ref, vnodeMaster, Some("key 1"), Get, majorityCoordinator, 500.millis))

      probe.watch(coordinator)
      probe.expectMsg(CoordinatorReply(130))
      probe.expectTerminated(coordinator)

      key1vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Get)(probe.ref)
        probe.expectMsg($CommandReply(130))
      })

      key2vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Get)(probe.ref)
        probe.expectMsg($CommandReply(0))
      })
    }

    "accumulate replies" in {
      val probe = TestProbe()

      VNodeMaster.lookup(vnodeMaster, "key 1")(probe.ref)
      val key1vnodes = probe.expectMsgType[LookupReply].vnodes

      VNodeActor.command(key1vnodes(0), Set(100))(probe.ref)
      VNodeActor.command(key1vnodes(1), Set(130))(probe.ref)
      VNodeActor.command(key1vnodes(2), Set(130))(probe.ref)
      probe.expectMsg($CommandReply(0))
      probe.expectMsg($CommandReply(0))
      probe.expectMsg($CommandReply(0))

      val coordinator = system.actorOf(Actor.props(probe.ref, vnodeMaster, Some("key 1"), Get, majorityCoordinator, 500.millis))

      probe.watch(coordinator)
      probe.expectMsg(CoordinatorReply(130))
      probe.expectTerminated(coordinator)
    }

    "not wait for dead vnodes" in {
      val probe = TestProbe()

      VNodeMaster.lookup(vnodeMaster, "key 1")(probe.ref)
      val key1vnodes = probe.expectMsgType[LookupReply].vnodes

      VNodeActor.command(key1vnodes(0), Set(100))(probe.ref)
      VNodeActor.command(key1vnodes(1), Set(100))(probe.ref)
      VNodeActor.command(key1vnodes(2), Set(100))(probe.ref)
      probe.expectMsg($CommandReply(0))
      probe.expectMsg($CommandReply(0))
      probe.expectMsg($CommandReply(0))

      val coordinator = system.actorOf(Actor.props(probe.ref, vnodeMaster, Some("key 1"), Get, majorityCoordinator, 500.millis))

      key1vnodes(0) ! PoisonPill
      key1vnodes(1) ! PoisonPill
      key1vnodes(2) ! PoisonPill

      probe.watch(coordinator)
      probe.expectMsg(200.millis, CoordinatorNoReply)
      probe.expectTerminated(coordinator)
    }

    "timeout on slow requests" in {
      val probe = TestProbe()

      val coordinator = system.actorOf(Actor.props(probe.ref, probe.ref, Some("key 1"), Get, majorityCoordinator, 200.millis))

      probe.watch(coordinator)
      probe.expectMsg(200.millis, Lookup("key 1"))
      probe.expectMsg(500.millis, CoordinatorNoReply)
      probe.expectTerminated(coordinator)
    }
  }
}
