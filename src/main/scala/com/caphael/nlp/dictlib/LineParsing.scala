package com.caphael.nlp.dictlib

/**
 * Created by caphael on 15/3/30.
 */
trait LineParsing{
  def parse(line:String):(Any,Any)
}

trait CharCharParsing extends LineParsing{
  def parse(line:String): (Char,Char) ={
    (line.head,line.last)
  }
}