package com.caphael.nlp.word

/**
 * Created by caphael on 15/4/15.
 */
class TermNode(private val id:String,
               private var prev:String=null,
               private var next:String=null)  extends Serializable with Comparable[TermNode]{
  def ID = id
  def PREVID = prev
  def PREVID_=(newval:String){prev=newval}
  def NEXTID = next
  def NEXTID_=(newval:String){next=newval}

  def +(other:TermNode):TermNode = {
    TermNode(id+other.id,prev,other.NEXTID)
  }

  override def compareTo(o: TermNode): Int = id.compareTo(o.id)
  override def hashCode() = id.hashCode

  override def equals(other:Any):Boolean = {
    other match {
      case o: TermNode => id.equals(o.ID)
      case _ => super.equals(other)
    }
  }

  override def toString:String = {
    {
      if (TermMetricNode.OUTNEIGHBOURS) {
        "[" + prev + "]" + id + "[" + next + "]"
      } else {
        id
      }
    }
  }

}

object TermNode extends Serializable{
  def apply(id:String,prev:String,next:String):TermNode = new TermNode(id,prev,next)
  def apply(id:String):TermNode = new TermNode(id)

  var OUTNEIGHBOURS = false

}