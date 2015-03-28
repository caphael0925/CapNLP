package com.caphael.nlp.metric

/**
 * Created by caphael on 15/3/26.
 */
class IndependenceMetric(private val id:String,private val prob:Double,private val ind:Double) extends Serializable{
  def word = id
  def probablility = prob
  def independence = ind

  override def toString = word+":Prob["+prob+"],Indep["+ind+"]"
}
object IndependenceMetric extends Serializable{
  def apply(id:String,prob:Double,ind:Double) = new IndependenceMetric(id,prob,ind)
}