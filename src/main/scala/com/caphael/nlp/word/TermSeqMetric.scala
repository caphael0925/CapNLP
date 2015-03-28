//package com.caphael.nlp.word
//
//import com.caphael.nlp.metric.MetricType.MetricType
//
//import scala.collection.mutable.{Map=>MetricMap}
//
//
///**
//* Created by caphael on 15/3/26.
//*/
//class TermSeqMetric(private val id:String,private val mm:MetricMap[MetricType,Double], private val ts:Array[TermMetric])
//  extends TermMetric(id,mm) with Serializable{
//
//  def TERMS = ts
//
//  override def toString = id+"["+mm+"]\nTerms:\n"+ts.mkString("\t","\n\t","")
//}
//
//object TermSeqMetric extends Serializable{
//  def apply(w:String,mm:MetricMap[MetricType,Double],ts:Array[TermMetric]):TermSeqMetric = new TermSeqMetric(w,mm,ts)
//}