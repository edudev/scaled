package scaled.coordinator

import math.ceil

trait AccumulateResponse[+Acc]
case class AccumulateContinue[Acc](acc: Acc) extends AccumulateResponse[Acc]
case class AccumulateReply(reply: Option[Any]) extends AccumulateResponse[Nothing]

trait Coordinator[Acc] {
  def init: Acc
  def accumulate(acc: Acc, reply: Any): AccumulateResponse[Acc]
  def finish(acc: Acc): Option[Any]
}

class MajorityCoordinator(replicationFactor: Int) extends Coordinator[List[Any]] {
  val majorityCount = ceil((replicationFactor + 1) / 2)

  def init = Nil
  def accumulate(acc0: List[Any], reply: Any): AccumulateResponse[List[Any]] = {
    val acc = reply :: acc0

    if (mostPopular(acc)._2 > this.majorityCount)
      AccumulateReply(Some(reply))
    else
      AccumulateContinue(acc)
  }

  def finish(acc: List[Any]): Option[Any] =
    if (acc.isEmpty)
      None
    else
      Some(mostPopular(acc)._1)

  private def mostPopular(acc: List[Any]): (Any, Int) =
    acc.groupBy(Predef.identity).mapValues(_.size).maxBy(_._2)
}
