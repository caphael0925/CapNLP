package com.caphael.nlp.predeal

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

  def charDicReplace(dic:Map[Char,Char])(input:String):String={
    input.map{x:Char=>{dic.getOrElse(x,x)}}.mkString
  }

}
