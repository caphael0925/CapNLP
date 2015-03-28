package com.caphael.nlp.util

import com.caphael.nlp.word.TermMetric
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import com.caphael.nlp.util.MetricUtils.MetricMap

/**
 * Created by caphael on 15/3/27.
 */
object SplitUtils{

  //Split parameters
  val PUNC_DILIM = """[\pP\p{Punct}\s]+"""

  object Lucene{
    def standardSplit(distinct:Boolean=false)(line:String):Array[String] = {
      val analyzer:Analyzer = new StandardAnalyzer()
      val tokenStream = analyzer.tokenStream("input",line)
      val termAttr = tokenStream.addAttribute(classOf[CharTermAttribute])
      tokenStream.reset()

      def terms:Stream[String]= Stream.cons( {tokenStream.incrementToken ;  new String(termAttr.buffer,0,termAttr.length) }, terms)
      val ret = terms.takeWhile(!_.isEmpty).toArray

      if (distinct) ret.distinct else ret
    }
  }

  def regexSplit(sep:String)(line:String): Array[String] ={
    line.split(sep).filter(!_.isEmpty)
  }

  def sentenceSplit = regexSplit(PUNC_DILIM) _

  def charSplit(distinct:Boolean=false)(line:String): Array[String] ={
    val chars = line.toArray
    (if(distinct) chars.distinct else chars)
      .map(_.toString)
      .filter(x=>PUNC_DILIM.r.findFirstIn(x)==None)
  }

  def neighbourSplit(neighbours:Int,distinct:Boolean)(tsm:TermMetric):Array[TermMetric]={
    val ret:Array[TermMetric] = tsm.SUBTERMS.sliding(neighbours).map{case(x)=>
      TermMetric(x.map(_.ID).mkString,MetricMap(),x)
    }.toArray

    if(distinct) ret.distinct else ret
  }

  def neighbourSplit(subSplit:(String)=>Array[String],neighbours:Int,distinct:Boolean)(line:String):Array[String]={
    val ret = subSplit(line).sliding(neighbours).toArray.map(_.mkString)
    if(distinct) ret.distinct else ret
  }}
