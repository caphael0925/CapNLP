package com.caphael.nlp.util

/**
 * Created by caphael on 15/3/26.
 */
object PredealUtils {


  def regexMatch(regex:String)(line:String):Boolean={
    line.matches(regex)
  }

  def regexReplace(regex:String)(replacement: String)(line:String):String={
    regex.r.replaceAllIn(line,replacement)
  }

}
