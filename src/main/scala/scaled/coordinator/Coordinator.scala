package scaled.coordinator

import math.ceil

trait AccumulateResponse[+Acc]
case class AccumulateContinue[Acc](acc: Acc) extends AccumulateResponse[Acc]
case class AccumulateReply[Reply](reply: Reply) extends AccumulateResponse[Nothing]

trait Coordinator[Acc] {
  def init: Acc
  def accumulate(acc: Acc, reply: Any): AccumulateResponse[Acc]
  def finish(acc: Acc): Any
}

class MajorityCoordinator(replicationFactor: Int) extends Coordinator[List[Any]] {
  val majorityCount = ceil((replicationFactor + 1) / 2)

  def init = Nil
  def accumulate(acc0: List[Any], reply: Any): AccumulateResponse[List[Any]] = {
    val acc = reply :: acc0

    if (mostPopular(acc)._2 > this.majorityCount)
      AccumulateReply(reply)
    else
      AccumulateContinue(acc)
  }

  def finish(acc: List[Any]): Any = mostPopular(acc)._1

  private def mostPopular(acc: List[Any]): (Any, Int) =
    acc.groupBy(Predef.identity).mapValues(_.size).maxBy(_._2)
}
