package scaled.vnode

trait Builder[Command, State] {
  def build: VNode[Command, State]
}
