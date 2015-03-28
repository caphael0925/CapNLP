package com.caphael.nlp.util

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * Created by caphael on 15/3/28.
 */
object HadoopUtils {
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

  def hdfsHandler(path:String) = new HDFSHandler(path)
}
