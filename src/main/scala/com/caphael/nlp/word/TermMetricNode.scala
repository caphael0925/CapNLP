package com.caphael.nlp.word

/**
 * Created by caphael on 15/4/14.
 */
class TermMetricNode(private val core:TermMetric,
                     private var prev:String=null,
                     private var next:String=null) extends Serializable with Comparable[TermMetricNode]{
  def CORE = core

  def PREVID = prev
  def PREVID_=(newval:String){prev=newval}
  def NEXTID = next
  def NEXTID_=(newval:String){next=newval}

  private val id = core.ID
  def ID = id
  def METRICS = core.METRICS
  def SUBTERMS = core.SUBTERMS

  override def equals(other:Any):Boolean = {
    other match {
      case o: TermMetricNode => id.equals(o.ID)
      case _ => super.equals(other)
    }
  }

  def +(other:TermMetricNode):TermMetricNode = {
    TermMetricNode(this.CORE+other.CORE,this.PREVID,other.NEXTID)
  }

  override def toString:String = {
    {
      if (TermMetricNode.OUTNEIGHBOURS) {
        "[" + prev + "]" + core.ID + "[" + next + "]"
      } else {
        core.ID
      }
    }+{
      if(TermMetricNode.OUTCORE) {":\n"+core.toString}else{""}
    }
  }

  override def hashCode() = id.hashCode

  override def compareTo(o: TermMetricNode): Int = id.compareTo(o.ID)

}

object TermMetricNode extends Serializable{
  def apply(core:TermMetric,prev:String,next:String) = new TermMetricNode(core,prev,next)
  def apply(core:TermMetric):TermMetricNode = new TermMetricNode(core)

  var OUTCORE = false
  var OUTNEIGHBOURS = false

}
