package com.caphael.nlp.word

import com.caphael.nlp.dictlib.CharCharDicHandler
import com.caphael.nlp.metric.MetricUtils
import MetricUtils._
import com.caphael.nlp.predeal.PredealUtils
import com.caphael.nlp.util.SplitUtils
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map

import com.caphael.nlp.metric.MetricType._

/**
* Created by caphael on 15/3/25.
*/
object WordMetricUtils extends Serializable{

  object DEFAULT{
    //App parameters
    val VERSION = "0.1.0"
    val APPNAME = "Word Discover "+VERSION


    //Independence calculator parameters
    val MAX_WORD_LENGTH = 6
    val MIN_WORD_FREQUENCY = 2
    val DELTA = 100
  }

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
  *   delta:Double          The threshold of independence(the ratio between probability of the term and product of subterms probabilities,it means a multiple), the term whose independence below it would be ignore
  * */
  def getMetrics(input:RDD[String]
                       ,sentencesCountL:Long=0L
                       ,maxWordLen:Int=DEFAULT.MAX_WORD_LENGTH
                       ,minWordFreq:Int=DEFAULT.MIN_WORD_FREQUENCY
                       ,delta:Double=DEFAULT.DELTA): RDD[TermMetric] ={
    val sentencesCount:Long = if(sentencesCountL>0L) {
      sentencesCountL
    } else {
      input.count
    }

    //Predeal
    val dicPath = "library/common"
    val dic = (new CharCharDicHandler).getDic(dicPath)
    val inputPredealed = input.map(PredealUtils.charDicReplace(dic)(_))

    //Term splitting
    val unFlatTerms:RDD[Array[String]] = inputPredealed.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty).cache

    //Calc the term-frequencies
    val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x.distinct)
    val flatInitTermMetrics:RDD[TermMetric] = flatTerms.map(x=>TermMetric(x,MetricMap()))
    val termFreq:RDD[TermMetric] = getFrequencies(flatInitTermMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
    val termProb:RDD[TermMetric] = getProbabilities(termFreq,sentencesCount)

    //Generate the term-prob dictionary
    val termProbDict = termProb.map(x=>(x.ID,x)).collect.toMap

    //Calc the term-sequences frequencies
    val termSeqProbs = for(i<-2 to maxWordLen) yield {

      //Initialize TermSeqMetric by
      val initTermSeqMetrics:RDD[TermMetric] = unFlatTerms.map{
        case(x) => TermMetric(x.mkString
          ,MetricMap()
          ,x.map(termProbDict.getOrElse(_,TermMetric.NULL)))
      }.filter(x=>x.SUBTERMS.forall(_!=TermMetric.NULL))

      val flatTermSeqMetrics:RDD[TermMetric] = initTermSeqMetrics.flatMap(SplitUtils.neighbourSplit(i,true)(_))
      val termSeqFreq:RDD[TermMetric] = getFrequencies(flatTermSeqMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
      val termSeqProb:RDD[TermMetric] = getProbabilities(termSeqFreq,sentencesCount)
      termSeqProb
    }

    //Union all the word probabilities
    val termSeqProbsUnion = termSeqProbs.reduceLeft(_.union(_))


    //Calculate independence rank of each character sequence and filter the entry above delta
    getRelevance(termSeqProbsUnion).filter(x=>x.METRICS(Relevance)>=delta)
  }

  def discover(input:RDD[String]): Unit ={
    //Generate the flattened Sentences
//    val flatSentences = flatten(input,SplitUtils.regexSplit())

    //Calc the sentences count
//    val sentencesCount:Long = flatSentences.count




  }

}


