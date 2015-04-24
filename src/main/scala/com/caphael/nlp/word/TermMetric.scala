package com.caphael.nlp.word

import com.caphael.nlp.metric.MetricMap
import com.caphael.nlp.metric.MetricType._
import java.util.NoSuchElementException

/**
* Created by caphael on 15/3/26.
*/
class TermMetric(private val id:String,
                 private val mm:MetricMap=TermMetric.MM_INIT,
                 private var st:Array[TermMetric]=TermMetric.ST_NULL) extends Serializable with Comparable[TermMetric]{

  def ID = id
  def METRICS = mm
  def SUBTERMS = st
  def SUBTERMS_=(atm:Array[TermMetric]){st=atm}

  def apply(k:MetricType)={
    mm(k)
  }

  val getOrElse = mm.getOrElse _

  override def equals(other:Any):Boolean = {
    other match {
      case o: TermMetric => id.equals(o.ID)
      case _ => super.equals(other)
    }
  }

  def subTermsToString = if (SUBTERMS!=TermMetric.ST_NULL) {"Subterms:\n"+st.mkString("\t","\n\t","")} else ""

  def +(other:TermMetric):TermMetric = {
    TermMetric(this.ID+other.ID,TermMetric.MM_INIT,Array(this,other))
  }

  override def toString = id+{
    if(TermMetric.OUTDETAIL){
      "["+mm+"]\n" + subTermsToString
    }else{""}
  }

  override def hashCode() = id.hashCode

  override def compareTo(o: TermMetric): Int = id.compareTo(o.ID)
}

object TermMetric extends Serializable{
  def apply(id:String,mm:MetricMap,st:Array[TermMetric]):TermMetric = new TermMetric(id,mm,st)
  def apply(id:String,mm:MetricMap):TermMetric = new TermMetric(id,mm)
  def apply(id:String):TermMetric = new TermMetric(id)

//  val TM_NULL = new TermMetric("",null,null)
  val ID_NULL:String = null
  val MM_NULL:MetricMap = MetricMap()
  val ST_NULL = null
  val TM_NULL:TermMetric = apply(ID_NULL,MM_NULL,ST_NULL)

  def MM_INIT = MetricMap()

  var OUTDETAIL = false
}