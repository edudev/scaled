package scaled

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.parasitic
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.PoisonPill
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

  def stop = master ! PoisonPill

  def command[Acc](key: Key, command: Command, coordinator: Coordinator[Acc])(implicit timeout: FiniteDuration): Future[Any] =
    ask(master, Master.CommandSingleKey(key, command, coordinator, timeout))(Timeout(timeout)).mapTo[CoordinatorReply].map(_.reply)(parasitic)

  def coverageCommand[Acc](command: Command, coordinator: Coordinator[Acc])(implicit timeout: FiniteDuration): Future[Any] =
    ask(master, Master.CommandCoverage(command, coordinator, timeout))(Timeout(timeout)).mapTo[CoordinatorReply].map(_.reply)(parasitic)
}
