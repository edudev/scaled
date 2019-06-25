package scaled.coordinator

trait Coordinator[Acc] {
  def init: Acc
  def accumulate(acc: Acc, reply: Any): Acc
  def finish(acc: Acc): Any
}

object MajorityCoordinator extends Coordinator[List[Any]] {
  def init = Nil
  def accumulate(acc: List[Any], reply: Any): List[Any] = reply :: acc
  def finish(acc: List[Any]): Any = acc.groupBy(Predef.identity).maxBy(_._2.size)._1
}
