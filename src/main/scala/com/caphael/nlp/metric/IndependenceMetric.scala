package com.caphael.nlp.metric

/**
 * Created by caphael on 15/3/26.
 */
class IndependenceMetric(val id:String,val prob:Double,val ind:Double) extends Serializable{
  def term = id
  def probablility = prob
  def independence = ind
}
object IndependenceMetric extends Serializable{
  def apply(id:String,prob:Double,ind:Double) = new IndependenceMetric(id,prob,ind)
}