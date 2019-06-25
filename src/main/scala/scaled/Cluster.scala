package scaled

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.util.Timeout
import akka.pattern.ask

import scaled.coordinator.Coordinator
import scaled.coordinator.Actor.CoordinatorReply

object Cluster {
  def apply[Key, Command, State](spec: Spec[Key, Command, State])(implicit system: ActorSystem): Cluster[Key, Command, State] =
    new Cluster(spec)(system)
}

class Cluster[Key, Command, State] private (master: ActorRef) {
  private def this(spec: Spec[Key, Command, State])(system: ActorSystem) =
    this(system.actorOf(Master.props(spec)))

  def command[Acc](key: Key, command: Command, coordinator: Coordinator[Acc])(timeout: FiniteDuration): Future[Any] =
    ask(master, Master.CommandM(key, command, coordinator))(Timeout(timeout)).mapTo[CoordinatorReply].map(_.reply)(parasitic)
}
