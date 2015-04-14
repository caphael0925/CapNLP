package com.caphael.nlp.metric

import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.word.TermMetric
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
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
    input.map{case(x:TermMetric)=>
      x.METRICS(Probability)=x.METRICS(Frequency)/totalL.toDouble
      x}
  }

  def getRelevance(input:RDD[TermMetric]):RDD[TermMetric] = {

    input.map{case(termSeqMetric)=>
      val subTerms:Array[TermMetric] = termSeqMetric.SUBTERMS
      val jointProb:Double = subTerms.map(_.METRICS(Probability)).reduce(_*_)
      termSeqMetric.METRICS(Relevance)=termSeqMetric.METRICS(Probability)/jointProb
      termSeqMetric
    }
  }

//  def getNeighbourEntropy(input:RDD[TermMetric]):RDD[TermMetric] = {
//
//
//  }
}