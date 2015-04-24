package com.caphael.nlp.word

import com.caphael.nlp.dictlib.CharCharDicHandler
import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.metric.MetricUtils._
import com.caphael.nlp.predeal.PredealUtils
import com.caphael.nlp.util.SplitUtils
import org.apache.spark.rdd.RDD

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
    val PARTITION_SIZE=2 * 100000

    //Metric calculator parameters
    val MAX_WORD_LENGTH = 6
    val MIN_WORD_FREQUENCY = 4
    val RELE_DELTA = 20
    val ETPY_DELTA = 1.5

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
  def discover(input:RDD[String]
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

    val minWordFreq=(charCount/100000.0).ceil+1

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
    //=================================Single Term Metric Dictionary!==========================================
    var singleTermMetricMap = singleTermProb.map(x=>(x.ID,x)).collect.toMap
    //=================================Yes it is!==============================================================

    //=================================Temp Multiple Terms Metric Dictionary!==================================
    var tempTermMetricMap =  singleTermMetricMap
    //=================================en en===================================================================

    //Initialize Terms into TermMetrics(With Sliding Grouping)
    ////Function that Initialize Array[String] to Array[TermMetricNode](which contained neighbourhood information)
    ////Array[String] => Array[TermNode]
    val initTermNodes:RDD[IndexedSeq[TermNode]] = unFlatTerms.map(initTermNodesFun(_))

    val res=for (curWordLen<-2 to maxWordLen) yield {
      //Function to get SubTermMetrics
      val getSubTermMetricsFun = getSubTermMetricsProtype(singleTermMetricMap,tempTermMetricMap) _

      ////Merge two neighbouring TermMetricNodes into one with Sliding Window(length=2,3...maxWordLen)
      val termNodesSliding:RDD[TermNode] = initTermNodes.filter(_.length>curWordLen).flatMap(SplitUtils.neighbourSplit(curWordLen,false)(_))

      //Calculate Frequency & Generate TermMetric Constructor (RDD[TermNode]=>RDD[TermMetric])
      val termMetricFreq:RDD[TermMetric] = getFrequenciesByTermNode(termNodesSliding).filter(x=>x(Frequency)>=minWordFreq)

      //Update TermMetric Objects with SubTerms (RDD[TermMetric]=>RDD[TermMetric])
      val termMetricFreqWithSubTerms=termMetricFreq.map(x => {x.SUBTERMS = getSubTermMetricsFun(x.ID);x}).filter(_.SUBTERMS!=TermMetric.ST_NULL)

      //Calculate Probability
      val termMetricProb:RDD[TermMetric] = getProbabilities(termMetricFreqWithSubTerms,charCount)

      //Calculate Relevance
      val termMetricRele:RDD[TermMetric] = getRelevance(termMetricProb).filter(x=>x(Relevance)>relevance_delta)

      //Calculate Entropy
      val termMetricEntr:RDD[TermMetric] = getEntropy(termNodesSliding)
      val termMetricEntr4Curr:RDD[TermMetric] = termMetricEntr.
        filter(x=>x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta)
      val termMetricEntr4Temp:RDD[TermMetric] = termMetricEntr.
        filter(x=> !(x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta))

      //Merge Metrics into one
      val currentResult:RDD[TermMetric] = termMetricRele.map(x=>(x.ID,x)).join(termMetricEntr4Curr.map(x=>(x.ID,x))).
        map{
        case(id,ms) => ms._1.METRICS++=ms._2.METRICS ; ms._1
      }

      //Fresh the TempTermMetricMap
      tempTermMetricMap = termMetricProb.map(x=>(x,null)).join(termMetricEntr4Temp.map(x=>(x,null))).map{
        case (tm,ns)=> (tm.ID,tm)
      }.collect.toMap

      currentResult
    }

    res.reduce(_.union(_))
 }

  def predeal(input:RDD[String]): RDD[String] ={
    val dicPath = "library/common"
    val dic = (new CharCharDicHandler).getDic(dicPath)
    input.map(PredealUtils.charDicReplace(dic)(_))
  }

  def createTermMetricDict(input:RDD[String]):HashMap[String,TermMetric]={
    HashMap(input.distinct.map(x=>(x,TermMetric(x))).collect:_*)
  }

//  def initTermMetricsProtype(singleDic: Map[String,TermMetric])(terms:Array[String]):Array[TermMetricNode]={
//
//    val ret:Array[TermMetricNode] = terms.map(x=>TermMetricNode(dic.getOrElse(x,TermMetric.TM_NULL)))
//    ret.reduce{(l:TermMetricNode,r:TermMetricNode)=>{l.NEXTID=r.ID;r.PREVID=l.ID;r}}
//    ret
//  }

  def getSubTermMetricsProtype(singleTermMetricDic: Map[String,TermMetric],
                             tempTermMetricDic: Map[String,TermMetric])
                            (terms:String):Array[TermMetric]={
    val leftCouple:Array[TermMetric] = Array(singleTermMetricDic.getOrElse(terms.take(1),TermMetric.TM_NULL),tempTermMetricDic.getOrElse(terms.tail,TermMetric.TM_NULL))
    val rightCouple:Array[TermMetric] = Array(tempTermMetricDic.getOrElse(terms.take(terms.length-1),TermMetric.TM_NULL),singleTermMetricDic.getOrElse(terms.last.toString,TermMetric.TM_NULL))

    val leftWeight = leftCouple.map(_.getOrElse(Probability,0.0)).reduce(_*_)
    val rightWeight = rightCouple.map(_.getOrElse(Probability,0.0)).reduce(_*_)

    val subterms = if (leftWeight == 0.0 || rightWeight == 0.0 ){
      TermMetric.ST_NULL
    }else if(leftWeight>rightWeight){
      leftCouple
    }else{
      rightCouple
    }

    subterms
  }

  def initTermNodesFun(terms:IndexedSeq[String]):IndexedSeq[TermNode]={
    val ret:IndexedSeq[TermNode] = terms.map(x=>TermNode(x))
    ret.reduce{
      (l:TermNode,r:TermNode)=>{l.NEXTID=r.ID;r.PREVID=l.ID;r}
    }
    ret
  }

}


