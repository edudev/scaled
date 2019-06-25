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

class MasterSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with WordSpecLike
  with BeforeAndAfter
  with BeforeAndAfterAll {

  def this() = {
    this(ActorSystem("MasterSpec"))
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

  import Actor.CoordinatorReply

  "A VNode Coordinator Master" should {
    import VNodeMaster.LookupReply
    import VNodeActor.$CommandReply
    import CounterVNode._

    "create coordinators, which forward commands" in {
      val probe = TestProbe()

      VNodeMaster.lookup(vnodeMaster, "key 1")(probe.ref)
      val key1vnodes = probe.expectMsgType[LookupReply].vnodes

      key1vnodes.foreach(vnode => {
        VNodeActor.command(vnode, Set(130))(probe.ref)
        probe.expectMsg($CommandReply(0))
      })

      val coordinatorMaster = system.actorOf(Master.props(vnodeMaster))

      Master.command(coordinatorMaster, "key 1", Get, MajorityCoordinator)(probe.ref)
      probe.expectMsg(CoordinatorReply(130))
    }
  }
}
