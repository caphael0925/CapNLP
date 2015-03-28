package com.caphael.nlp.util

import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.word.TermMetric
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{HashMap, Map}
/**
* Created by caphael on 15/3/27.
*/
object MetricUtils {
  def MetricMap(entry:(MetricType,Double)*):HashMap[MetricType,Double] = {
    val ret = HashMap[MetricType, Double](entry:_*)
    ret(Counter)=1.0
    ret
  }

  def getFrequencies(input:RDD[TermMetric]):RDD[TermMetric]={
    input.map(x=>(x,x.METRICS(Counter))).reduceByKey(_+_).map{case(tm,f)=>tm(Frequency)=f;tm}
  }

  def getProbabilities(input:RDD[TermMetric],totalL:Long):RDD[TermMetric]={
    input.foreach(x=>x.METRICS(Probability)=x.METRICS(Frequency)/totalL.toDouble)
    input
  }

  def getIndependence(input:RDD[TermMetric]):RDD[TermMetric] = {

    def calcIndependence(termSeqMetric: TermMetric): Unit ={
      val subTerms:Array[TermMetric] = termSeqMetric.SUBTERMS
      val jointProb:Double = subTerms.map(_.METRICS(Probability)).reduce(_*_)
      termSeqMetric.METRICS(Independence)=jointProb/termSeqMetric.METRICS(Probability)
    }

    input.foreach(calcIndependence(_))
    input
  }
}
