package scaled

import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{AsyncFlatSpec, Matchers, BeforeAndAfter, BeforeAndAfterAll}

import akka.actor.{ ActorSystem }

import scaled.coordinator.MajorityCoordinator

import scaled.samples.KvVNode

class KvClusterSpec(system: ActorSystem)
  extends AsyncFlatSpec
  with Matchers
  with BeforeAndAfter
  with BeforeAndAfterAll {

  def this() = {
    this(ActorSystem("KvClusterSpec"))
  }

  override def afterAll: Unit = {
    Await.result(system.terminate, 1.minute)
  }

  var cluster: Cluster[String, KvVNode.Command, Map[String, Int]] = _

  before {
    cluster = Cluster(KvVNode.spec[Int])(system)
  }

  after {
    cluster.stop
  }

  val majorityCoordinator = new MajorityCoordinator(KvVNode.spec.replicationFactor)
  implicit val timeout: FiniteDuration = 5.seconds
  import KvVNode._

  "Kv Cluster" should "be empty at first" in {
    cluster.command("key 1", Get("key 1"), majorityCoordinator) map { result =>
      result shouldEqual None
    }
  }

  it should "be able to store keys" in {
    val cluster = Cluster(KvVNode.spec[Int])(system)

    cluster.command("key 1", Put("key 1", 10), majorityCoordinator) map { result =>
      result shouldEqual None
    } flatMap(_ => cluster.command("key 1", Get("key 1"), majorityCoordinator) map { result =>
      result shouldEqual Some(10)
    })
  }

  it should "be able to list keys" in {
    val cluster = Cluster(KvVNode.spec[Int])(system)

    cluster.command("key 1", Put("key 1", 10), majorityCoordinator) map { result =>
      result shouldEqual None
    } flatMap(_ => cluster.command("key 1", List, majorityCoordinator) map { result =>
      result shouldEqual Set("key 1")
    })
  }
}
