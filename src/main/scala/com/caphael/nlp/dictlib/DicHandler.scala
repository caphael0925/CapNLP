package com.caphael.nlp.dictlib

/**
 * Created by caphael on 15/3/30.
 */
abstract class DicHandler extends DicHandling{

  override def getDic(dicpath:String):Any={
    val ret = load(read(dicpath))
    log("Loading Dictionary Succeeded!!")
    ret
  }

  def log(msg:String): Unit ={
    println(msg)
  }
}

class CharCharDicHandler extends DicHandler with ReadFromFile[Char,Char] with LoadToMap[Char,Char] with CharCharParsing{
  override def getDic(dicpath:String):Map[Char,Char]={
    super.getDic(dicpath) match{
      case ret:Map[Char,Char] => ret
    }
  }
}
