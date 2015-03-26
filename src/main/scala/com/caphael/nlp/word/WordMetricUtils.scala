package com.caphael.nlp.word

import com.caphael.nlp.metric.IndependenceMetric
import org.apache.spark.rdd.RDD

/**
 * Created by caphael on 15/3/25.
 */
class WordMetricUtils extends Serializable{

  /*
  * Flatten the lines by <split> function
  * param:
  *   input:RDD[String]   Lines
  *   split:(String)=>Array[String]   Function to split lines into array
  * */
  def flatten(input:RDD[String],split:(String)=>Array[String]):RDD[String]={
    input.flatMap(split(_))
  }

  def termSplit(sep:String=WordMetricUtils.PUNC_DILIM)(line:String): Array[String] ={
    line.split(sep).filter(!_.isEmpty)
  }

  def charSplit(distinct:Boolean=false)(line:String): Array[String] ={
    val chars = line.toArray
    (if(distinct) chars.distinct else chars)
      .map(_.toString)
      .filter(x=>WordMetricUtils.PUNC_DILIM.r.findFirstIn(x)==None)
  }

  def neighbourSplit(neighbours:Int,distinct:Boolean=false)(line:String):Array[String]={
    val ret = line.sliding(neighbours).toArray
    if(distinct) ret.distinct else ret
    }

  def getFrequencies(input:RDD[String]): RDD[(String,Long)] ={
    input.map(x=>(x,1L)).reduceByKey(_+_)
  }

  def getProbabilities(input:Any,totalL:Long):RDD[(String,Double)]={
    val total = totalL.toDouble

    val in:RDD[(String,Long)] = input match{
      case input:RDD[String] => getFrequencies(input)
      case input:RDD[(String,Long)] => input
    }

    in.map{case(str,freq)=>(str,freq/total)}
  }

  /*
  * Calculate Independence between a pair of neighbour characters
  * param:
  *   input:RDD[String]     Sentences
  *   sentencesCount:Long   Count of sentences
  *   maxWordLen:Int     The maximum of the words to discovery, the words whose length above it would be ignore
  *   delta:Double          The threshold of independence of chars in one word, the words whose chars independence below it would be ignore
  * */
  def calcIndependence(input:RDD[String]
                       ,sentencesCountL:Long=0L
                       ,maxWordLen:Int=WordMetricUtils.DEFAULT_MAX_WORD_LENGTH
                       ,delta:Double=WordMetricUtils.DEFAULT_DELTA): RDD[IndependenceMetric] ={
    val sentencesCount:Long = if(sentencesCountL!=0L) {
      sentencesCountL
    } else {
      input.count
    }

    //Calc the char-frequencies
    val flatCharInput = flatten(input,charSplit(true))
    val charProb = getProbabilities(flatCharInput,sentencesCount)

    //Calc the char-sequences frequencies
    val charSeqProbKeys = (2 to maxWordLen).toArray
    val charSeqProbValues = for(i<-2 to maxWordLen) yield {
      val flatCharSeq = flatten(input,neighbourSplit(i,true))
      getProbabilities(flatCharSeq,sentencesCount)
    }

    //Union all the word probabilities
    val charSeqProbs = charSeqProbValues.reduceLeft(_.union(_))

    //Generate the char-prob dictionary
    val charProbDict = charProb.collect.toMap

    def getIndependence(charSeq:String,prob:Double):IndependenceMetric = {
      val jointProb = (for(c<-charSeq) yield {
        charProbDict.getOrElse(c.toString,prob)
      }).reduce(_*_)
      IndependenceMetric(charSeq,prob,1-jointProb/prob)
    }

    //Calculate independence rank of each character sequence and filter the entry above delta
    charSeqProbs.map{case(str,prob)=>getIndependence(str,prob)}.filter{m=>m.independence>=delta}
  }

  def discover(input:RDD[String]): Unit ={
    //Generate the flattened Sentences
    val flatSentences = flatten(input,termSplit())

    //Calc the sentences count
    val sentencesCount:Long = flatSentences.count




  }

}


object WordMetricUtils {
  //App parameters
  val VERSION = "0.1.0"
  val APPNAME = "Word Discover "+VERSION

  //Split parameters
  val PUNC_DILIM = """[\pP\p{Punct}\s]+"""

  //Independence calculator parameters
  val DEFAULT_MAX_WORD_LENGTH = 6
  val DEFAULT_DELTA = 0.9

  def apply():WordMetricUtils = new WordMetricUtils

}

