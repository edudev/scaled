package scaled.vnode

trait VNode[Command, State] {
  def init: State
  def terminate(state: State): Unit = ()

  def handleCommand(sender: Sender, command: Command, state: State): CommandResponse[State]
}
