package com.caphael.nlp.metric

import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.word.{TermNode, TermMetricNode, TermMetric}
import org.apache.spark.rdd.RDD

import scala.math.log


/**
* Created by caphael on 15/3/27.
*/
object MetricUtils {

//  def getFrequencies(input:RDD[TermMetric]):RDD[TermMetric]={
//    input.map(x=>(x,x.METRICS(Counter))).reduceByKey(_+_).map{case(tm,f)=>tm.METRICS(Frequency)=f;tm}
//  }

  def getFrequenciesByString(input:RDD[String]):RDD[TermMetric]={

    input.map(x=>(x,1.0)).reduceByKey(_+_).map{
      case (id:String,f:Double)=>TermMetric(id,MetricMap(Frequency->f))
    }

  }

  def getFrequenciesByTermMetric(input:RDD[TermMetric]):RDD[TermMetric]={
    input.map(x=>(x,1.0)).reduceByKey(_+_).map{
      case(tm,f)=>tm.METRICS(Frequency)=f;
                  tm
    }
  }

  def getFrequenciesByTermNode(input:RDD[TermNode]):RDD[TermMetric]={
    input.map(x=>(x,1.0)).reduceByKey(_+_).map{
      case(tn,f)=>TermMetric(tn.ID,MetricMap(Frequency->f))
    }
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

  def getEntropy(input:RDD[TermMetricNode]): RDD[TermMetric] ={
    //Functions
    def _getStringArrayEntropy(strArr:IndexedSeq[String]):Double = {
      val (strArrNull,strArrNoneNull) = strArr.partition(_==null)
      val countVec = strArrNoneNull.groupBy(x=>x).map(_._2.length.toDouble)
      val countAll = strArrNoneNull.length.toDouble
      val ret = -countVec.map(_/countAll).map(x=>x*log(x)).sum

      if(!strArrNull.isEmpty){
        ret+1
      }else{
        ret
      }
    }

    def _getTermMetricNodeArrayEntropy(tmnArr:IndexedSeq[TermMetricNode]):MetricMap = {
      val (leftVec,rightVec) = tmnArr.map(x=>(x.PREVID,x.NEXTID)).unzip
      val ret = MetricMap()
      ret(LeftEntropy) = _getStringArrayEntropy(leftVec)
      ret(RightEntropy) = _getStringArrayEntropy(rightVec)
      ret
    }

    input.groupBy(x=>x.CORE).map{
      case(tm,tmnSeq)=>tm.METRICS++=_getTermMetricNodeArrayEntropy(tmnSeq.toIndexedSeq);tm
    }

  }

//  def getNeighbourEntropy(input:RDD[TermMetric]):RDD[TermMetric] = {
//
//
//  }
}
