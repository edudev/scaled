package scaled

import scala.util.hashing.MurmurHash3
import scala.concurrent.duration._
import scala.concurrent.Await

import org.scalatest.{AsyncFlatSpec, Matchers, BeforeAndAfterAll}

import akka.actor.{ ActorSystem }

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

  import scaled.coordinator.Actor.CoordinatorReply

  "A Cluster" should "be ready for commands" in {
    import CounterVNode._

    val cluster = Cluster(CounterVNode.builder)(MurmurHash3.stringHashing, system)

    cluster.command("key 1", Set(130))(5.seconds) flatMap { result =>
      result shouldEqual 0

      cluster.command("key 1", Get)(5.seconds) map { result =>
        result shouldEqual 130
      }
    }
  }
}
