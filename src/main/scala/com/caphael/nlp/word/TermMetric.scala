package com.caphael.nlp.word

import com.caphael.nlp.metric.MetricType.MetricType

import scala.collection.mutable.{HashMap=>MetricMap}
/**
* Created by caphael on 15/3/26.
*/
class TermMetric(private val id:String,private val mm:MetricMap[MetricType,Double], private val st:Array[TermMetric]) extends Serializable{

  def ID = id
  def METRICS = mm
  def SUBTERMS = st

  def contains = mm.contains _
  def apply = mm.apply _
  def update = mm.update _

  override def equals(other:Any):Boolean = {
    other match {
      case o: TermMetric => id.equals(o.ID)
      case _ => super.equals(other)
    }
  }

  def subTermsToString = if (SUBTERMS!=null) {"Subterms:\n"+st.mkString("\t","\n\t","")} else ""

  override def toString = id+"["+mm+"]\n" + subTermsToString

  override def hashCode() = id.hashCode
}

object TermMetric extends Serializable{
  def apply(id:String,mm:MetricMap[MetricType,Double],st:Array[TermMetric]):TermMetric = new TermMetric(id,mm,st)
  def apply(id:String,mm:MetricMap[MetricType,Double]):TermMetric = apply(id,mm,null)
  def apply(id:String):TermMetric = apply(id,MetricMap[MetricType,Double]())

  val NULL = new TermMetric("\000",null,null)
}