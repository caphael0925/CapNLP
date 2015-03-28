//package com.caphael.nlp.metric
//
//import scala.collection.mutable.HashMap
//
///**
// * Created by caphael on 15/3/27.
// */
//class MetricMap extends HashMap[String,Double]{
//  def extend(input:Array[(String,Double)]): MetricMap ={
//    input.foreach(this+=_)
//    this
//  }
//
//  override def toString =  {for((k,v)<-this) yield k+":"+v}.mkString(",")
//}
//
//object MetricMap extends HashMap{
//  def apply(entry:(String,Double)*):MetricMap = {
//    super.apply(entry)
//  }
//
//  def apply():MetricMap = new MetricMap
//}