package com.caphael.nlp.util

import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.word.{TermSeqMetric, TermMetric}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, Map}
/**
* Created by caphael on 15/3/27.
*/
object MetricUtils {
  def MetricMap(entry:(MetricType,Double)*):Map[MetricType,Double] = {
    HashMap[MetricType, Double](entry:_*,(Counter,1.0))
  }

  def getFrequencies(input:RDD[TermMetric]):RDD[TermMetric]={
    input.map(x=>(x,x(Counter))).reduceByKey(_+_).map{case(tm,f)=>tm(Frequency)=f;tm}
  }

  def getProbabilities(input:RDD[TermMetric],totalL:Long):RDD[TermMetric]={
    input.foreach(x=>x(Probability)=x(Frequency)/totalL.toDouble)
    input
  }

  def getIndependence(input:RDD[TermMetric]):RDD[TermMetric] = {

    def calcIndependence(termSeqMetric: TermMetric): Unit ={
      val jointProb:Double = termSeqMetric.SUBTERMS.reduce{case(l,r)=>l(Probability)*r(Probability)}
      termSeqMetric(Independence)=jointProb/termSeqMetric(Probability)
    }

    input.foreach(calcIndependence(_))
    input
  }
}
