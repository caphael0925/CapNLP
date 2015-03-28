package com.caphael.nlp.word

import com.caphael.nlp.util.MetricUtils._
import com.caphael.nlp.util.SplitUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

import com.caphael.nlp.metric.MetricType._

/**
* Created by caphael on 15/3/25.
*/
object WordMetricUtils extends Serializable{

  //App parameters
  val VERSION = "0.1.0"
  val APPNAME = "Word Discover "+VERSION


  //Independence calculator parameters
  val DEFAULT_MAX_WORD_LENGTH = 6
  val DEFAULT_DELTA = 0.9


  /*
  * Flatten the lines by <split> function
  * param:
  *   input:RDD[String]   Lines
  *   split:(String)=>Array[String]   Function to split lines into array
  * */
  def flatten(input:RDD[String],split:(String)=>Array[String]):RDD[String]={
    input.flatMap(split(_))
  }

  /*
  * Calculate Independence between a pair of neighbour characters
  * param:
  *   input:RDD[String]     Sentences
  *   sentencesCount:Long   Count of sentences
  *   maxWordLen:Int     The maximum of the words to discovery, the words whose length above it would be ignore
  *   delta:Double          The threshold of independence of chars in one word, the words whose chars independence below it would be ignore
  * */
  def getMetrics(input:RDD[String]
                       ,sentencesCountL:Long=0L
                       ,maxWordLen:Int=WordMetricUtils.DEFAULT_MAX_WORD_LENGTH
                       ,delta:Double=WordMetricUtils.DEFAULT_DELTA): RDD[TermMetric] ={
    val sentencesCount:Long = if(sentencesCountL>0L) {
      sentencesCountL
    } else {
      input.count
    }

    //Term splitting
    val unFlatTerms:RDD[Array[String]] = input.map(SplitUtils.Lucene.standardSplit(false)(_)).cache

    //Calc the term-frequencies
    val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x.distinct)
    val flatInitTermMetrics:RDD[TermMetric] = flatTerms.map(x=>TermMetric(x,MetricMap()))
    val termFreq:RDD[TermMetric] = getFrequencies(flatInitTermMetrics)
    val termProb:RDD[TermMetric] = getProbabilities(termFreq,sentencesCount)

    //Generate the term-prob dictionary
    val termProbDict = termProb.map(x=>(x.ID,x)).collect.toMap

    //Calc the term-sequences frequencies
    val termSeqProbs = for(i<-2 to maxWordLen) yield {

      //Initialize TermSeqMetric by
      val initTermSeqMetrics:RDD[TermMetric] = unFlatTerms.map{
        case(x) => TermMetric(x.mkString
          ,MetricMap()
          ,x.map(termProbDict(_)))
      }

      val flatTermSeqMetrics:RDD[TermMetric] = initTermSeqMetrics.flatMap(SplitUtils.neighbourSplit(i,true)(_))
      val termSeqFreq:RDD[TermMetric] = getFrequencies(flatTermSeqMetrics)
      val termSeqProb:RDD[TermMetric] = getProbabilities(termSeqFreq,sentencesCount)
      termSeqProb
    }

    //Union all the word probabilities
    val termSeqProbsUnion = termSeqProbs.reduceLeft(_.union(_))



    //Calculate independence rank of each character sequence and filter the entry above delta
    getIndependence(termSeqProbsUnion).filter(x=>x(Independence)>=delta)
  }

  def discover(input:RDD[String]): Unit ={
    //Generate the flattened Sentences
//    val flatSentences = flatten(input,SplitUtils.regexSplit())

    //Calc the sentences count
//    val sentencesCount:Long = flatSentences.count




  }

}


