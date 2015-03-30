package com.caphael.nlp.hadoop

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

/**
 * Created by caphael on 15/3/30.
 */
class HDFSHandler(private val outpath:String) extends Serializable{

  private final val hdfshdl = FileSystem.get(URI.create(outpath),new Configuration())

  def toPath(filepath:Any):Path={
    filepath match {
      case path:String=> new Path(path)
      case path:Path=>path
      case _=>null
    }
  }

  def exists(filepath:Any):Boolean={
    hdfshdl.exists(toPath(filepath))
  }

  def listFiles(filepath:Any):Array[Path]={
    for(s<-hdfshdl.listStatus(toPath(filepath))) yield s.getPath
  }

  def delete(filepath:Any,recursive:Boolean=true)={
    hdfshdl.delete(toPath(filepath),recursive)
  }
}
object HDFSHandler{
  def apply(path:String) = new HDFSHandler(path)
}
