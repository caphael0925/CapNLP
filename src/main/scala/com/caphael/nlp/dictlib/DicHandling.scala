package com.caphael.nlp.dictlib

import java.io.{FilenameFilter, File}

import scala.io.Source

/**
 * Created by caphael on 15/3/30.
 */
trait DicHandling{
  def read(dicpath:String):Any
  def load(entries:Any):Any
  def getDic(dicpath:String):Any
}

trait ReadFromFile[K,V] extends DicHandling with LineParsing{
  //Parameters
  //  val updatePoint = 1000

  override def read(dicpath:String): Array[(K,V)] ={
    val file = new File(dicpath)
    val filter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        name.endsWith(".dic")
      }
    }

    val ret = if(file.isDirectory){
      val rets:Array[Array[(Any,Any)]]=for(subf<-getSubFiles(dicpath,filter)) yield {
        readFile(dicpath+"/"+subf)
      }
      rets.reduce(_++_)
    }else{
      readFile(dicpath)
    }

    ret match{
      case ret:Array[(K,V)] => ret
    }

  }

  def getSubFiles(path:String,filter:FilenameFilter=null): Array[String] ={
    val dir = new File(path)
    if(null==filter) dir.list else dir.list(filter)
  }

  def readFile(path:String): Array[(Any,Any)] ={
    val source = Source.fromFile(path,"UTF-8")
    val lines = source.getLines()

    val dicEntries = for(line<-lines) yield{
      parse(line)
    }
    val ret:Array[(Any,Any)] = dicEntries.toArray
    source.close
    ret
  }
}

trait LoadToMap[K,V] extends DicHandling{
  override def load(entries:Any): Map[K,V] ={
    entries match{
      case entries:Array[(K,V)] => entries.toMap
      case entries:Iterator[(K,V)] => entries.toMap
    }
  }
}