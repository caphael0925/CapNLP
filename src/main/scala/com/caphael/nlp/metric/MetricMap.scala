package com.caphael.nlp.metric

import com.caphael.nlp.metric.MetricType._
import scala.collection.mutable.HashMap

/**
 * Created by caphael on 15/4/14.
 */
class MetricMap extends HashMap[MetricType,Double]{
  def apply(k:MetricType,default:Double=0.0)={
    getOrElse(k,default)
  }
}

object MetricMap{
  def apply(init:(MetricType,Double)):MetricMap={
    val ret = new MetricMap
    ret(init._1)=init._2
    ret
  }

  def apply():MetricMap= new MetricMap

}
