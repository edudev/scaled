package scaled

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.duration.FiniteDuration
import scala.util.hashing.Hashing

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.util.Timeout
import akka.pattern.ask

import scaled.vnode.Builder
import scaled.coordinator.Coordinator
import scaled.coordinator.Actor.CoordinatorReply

object Cluster {
  def apply[Key, Command, State](builder: Builder[Command, State])(implicit hashing: Hashing[Key], system: ActorSystem): Cluster[Key, Command, State] =
    new Cluster(builder)(hashing, system)
}

class Cluster[Key, Command, State] private (master: ActorRef) {
  private def this(builder: Builder[Command, State])(implicit hashing: Hashing[Key], system: ActorSystem) =
    this(system.actorOf(Master.props(builder)(hashing)))

  def command[Acc](key: Key, command: Command, coordinator: Coordinator[Acc])(timeout: FiniteDuration): Future[Any] =
    ask(master, Master.CommandM(key, command, coordinator))(Timeout(timeout)).mapTo[CoordinatorReply].map(_.reply)(parasitic)
}
