package scaled

import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{AsyncFlatSpec, Matchers, BeforeAndAfterAll}

import akka.actor.{ ActorSystem }

import scaled.coordinator.MajorityCoordinator

import scaled.samples.CounterVNode

class ClusterSpec(system: ActorSystem)
  extends AsyncFlatSpec
  with Matchers
  with BeforeAndAfterAll {

  def this() = {
    this(ActorSystem("ClusterSpec"))
  }

  override def afterAll: Unit = {
    Await.result(system.terminate, 1.minute)
  }

  "A Cluster" should "be ready for commands" in {
    import CounterVNode._

    val cluster = Cluster(CounterVNode.spec)(system)

    cluster.command("key 1", Set(130), new MajorityCoordinator(CounterVNode.spec.replicationFactor))(5.seconds) map { result =>
      result shouldEqual 0
    } flatMap(_ => cluster.command("key 1", Get, new MajorityCoordinator(CounterVNode.spec.replicationFactor))(5.seconds) map { result =>
      result shouldEqual 130
    })
  }
}
