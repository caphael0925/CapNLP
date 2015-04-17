package com.caphael.nlp.word

import com.caphael.nlp.dictlib.CharCharDicHandler
import com.caphael.nlp.metric.MetricUtils
import com.caphael.nlp.metric.MetricUtils._
import com.caphael.nlp.predeal.PredealUtils
import com.caphael.nlp.util.SplitUtils
import org.apache.spark.rdd.RDD

import com.caphael.nlp.metric.MetricType._

import scala.collection.mutable.HashMap

/**
* Created by caphael on 15/3/25.
*/
object WordMetricUtils extends Serializable{

  object DEFAULT{
    //App parameters
    val VERSION = "0.1.0"
    val APPNAME = "Word Discover "+VERSION

    //Spark parameters
    val PARTITION_SIZE=2 * 10^5

    //Metric calculator parameters
    val MAX_WORD_LENGTH = 6
    val MIN_WORD_FREQUENCY = 3
    val RELE_DELTA = 10
    val ETPY_DELTA = 1

  }

  val neighbourSplit = SplitUtils.neighbourSplit(2,false) _

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
                       ,charCountL:Long=0L
                       ,maxWordLen:Int=DEFAULT.MAX_WORD_LENGTH
                       ,minWordFreq:Int=DEFAULT.MIN_WORD_FREQUENCY
                       ,relevance_delta:Double=DEFAULT.RELE_DELTA
                       ,entropy_delta:Double=DEFAULT.ETPY_DELTA): RDD[TermMetric] ={
    val charCount:Long = if(charCountL>0L) {
      charCountL
    } else {
      input.map(_.length).reduce(_+_)
    }

    val partitionNum:Int = charCount.toInt/DEFAULT.PARTITION_SIZE
    val partInput = input.repartition(partitionNum)

    //Predeal
    val inputPredealed = predeal(partInput);

    //Term splitting
    val unFlatTerms:RDD[Array[String]] = inputPredealed.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty)
    val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x)

    //Initialize Single Term Metric Dictionary
    val singleTermFreq:RDD[TermMetric] = getFrequenciesByString(flatTerms)
    val singleTermProb:RDD[TermMetric] = getProbabilities(singleTermFreq,charCount)
    ////Generate the term-prob dictionary(1 char)
    val singleTermMetricMap = singleTermProb.map(x=>(x.ID,x)).collect.toMap

    //Initialize Terms into TermMetrics(With Sliding Grouping)
    ////Function that Initialize Array[String] to Array[TermMetricNode](which contained neighbourhood information)
    val initTermMetricsFun:(Array[String])=>Array[TermMetricNode] = initTermMetricsProtype(singleTermMetricMap) _
    ////Array[String] => Array[TermMetricNode]
    val initTermMetricNodes:RDD[Array[TermMetricNode]] = unFlatTerms.map(initTermMetricsFun(_))
    ////Merge two neighbouring TermMetricNodes into one with Sliding Window(length=2)
    val termMetricNodesSliding:RDD[TermMetricNode] = initTermMetricNodes.filter(_.length>1).flatMap(neighbourSplit(_))

    //Initialize Neighbouring Terms Metric Dictionary
    val neighbourTermFreq:RDD[TermMetric] = getFrequenciesByTermMetric(termMetricNodesSliding.map(_.CORE)).filter(x=>x.METRICS(Frequency)>=minWordFreq)
    val neighbourTermProb:RDD[TermMetric] = getProbabilities(neighbourTermFreq,charCount)

    //Calculate Relevance between Neighbouring Terms
    val neighbourTermRele:RDD[TermMetric] = getRelevance(neighbourTermProb).filter(x=>x.METRICS(Relevance)>relevance_delta)

    val neighbourTermEtpy:RDD[TermMetric] = getEntropy(termMetricNodesSliding).
      filter(x=>x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta)

    val neighbourTermMetric:RDD[TermMetric] = neighbourTermEtpy.map(x=>(x.ID,x)).join(neighbourTermRele.map(x=>(x.ID,x))).
      map{
      case(id,ms) => ms._1.METRICS++=ms._2.METRICS ; ms._1
    }

    neighbourTermMetric
 }

  def predeal(input:RDD[String]): RDD[String] ={
    val dicPath = "library/common"
    val dic = (new CharCharDicHandler).getDic(dicPath)
    input.map(PredealUtils.charDicReplace(dic)(_))
  }

  def createTermMetricDict(input:RDD[String]):HashMap[String,TermMetric]={
    HashMap(input.distinct.map(x=>(x,TermMetric(x))).collect:_*)
  }

  def initTermMetricsProtype(dic: Map[String,TermMetric])(terms:Array[String]):Array[TermMetricNode]={

    val ret:Array[TermMetricNode] = terms.map(x=>TermMetricNode(dic.getOrElse(x,TermMetric.TM_NULL)))
    ret.reduce{(l:TermMetricNode,r:TermMetricNode)=>{l.NEXTID=r.ID;r.PREVID=l.ID;r}}
    ret
  }

  def initTermNodes(terms:IndexedSeq[String]):IndexedSeq[TermNode]={
    val ret:IndexedSeq[TermNode] = terms.map(x=>TermNode(x))
    ret.reduce{
      (l:TermNode,r:TermNode)=>{l.NEXTID=r.ID;r.PREVID=l.ID;r}
    }
    ret
  }

  def discover(input:RDD[String]): Unit ={
    //Generate the flattened Sentences
//    val flatSentences = flatten(input,SplitUtils.regexSplit())

    //Calc the sentences count
//    val sentencesCount:Long = flatSentences.count




  }
}


