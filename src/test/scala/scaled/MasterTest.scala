package scaled

import scala.concurrent.duration._

import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, WordSpecLike, Matchers }
import akka.actor.{ ActorSystem, ActorRef, PoisonPill }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import akka.pattern.ask

import scaled.vnode.{ Master => VNodeMaster }
import scaled.vnode.{ Actor => VNodeActor }

import scaled.coordinator.MajorityCoordinator

import scaled.samples.CounterVNode

class MasterSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with WordSpecLike
  with BeforeAndAfterAll {

  def this() = {
    this(ActorSystem("MasterSpec"))
  }

  override def afterAll: Unit = {
    shutdown(system)
  }

  import scaled.coordinator.Actor.CoordinatorReply

  "A Scaled Master" should {
    import CounterVNode._

    "create a vnode master and a coordinator master, and forward commands" in {
      val master = system.actorOf(Master.props(CounterVNode.spec))

      val probe = TestProbe()

      Master.command(master, "key 1", Set(130), new MajorityCoordinator(CounterVNode.spec.replicationFactor))(probe.ref)
      probe.expectMsg(CoordinatorReply(0))

      Master.command(master, "key 1", Get, new MajorityCoordinator(CounterVNode.spec.replicationFactor))(probe.ref)
      probe.expectMsg(CoordinatorReply(130))
    }
  }
}
