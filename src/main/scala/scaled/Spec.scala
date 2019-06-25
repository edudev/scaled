package scaled

import scala.util.hashing.Hashing

import scaled.vnode.VNode

trait Spec[Key, Command, State] {
  def replicationFactor: Int
  def build: VNode[Command, State]
  def hashing: Hashing[Key]
}
