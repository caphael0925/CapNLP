package com.caphael.nlp.metric

import com.caphael.nlp.metric.MetricType.MetricType

/**
 * Created by caphael on 15/3/27.
 */
trait MetricTypeToString extends Map[MetricType,Double] {
  override def toString:String = {for((k,v)<-this) yield k+":"+v}.mkString(",")
}
