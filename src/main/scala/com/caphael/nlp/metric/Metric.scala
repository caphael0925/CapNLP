package com.caphael.nlp.metric

/**
 * Created by caphael on 15/3/26.
 */
class Metric(private val n:String)(private val v:Double) extends Serializable{
  def NAME = n
  def VALUE = v

  override  def toString:String = n + "["+v+"]"
}
object Metric{

  def apply(n:String)(v:Double):Metric = new Metric(n)(v)
  def unapply(input:(String,Double)) = Some((input._1,input._2))
}
