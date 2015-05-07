package com.caphael.nlp.util

/**
 * Created by caphael on 15/5/5.
 */

import java.io.PrintWriter


object IOUtils {

  def save2TextFile(data:IndexedSeq[String],file:String): Unit ={
    val out = new PrintWriter(file)
    data.foreach(line=>out.println(line))
    out.close()
  }

}
