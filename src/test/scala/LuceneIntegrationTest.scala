import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

/**
 * Created by caphael on 15/3/26.
 */
object LuceneIntegrationTest extends App{

  val line = "我爱北京天安门,天安门上Sun生生。哈哈哈~哦啊"
  val analyzer:Analyzer = new StandardAnalyzer()
  val tokenStream = analyzer.tokenStream("input",line)
  val termAttr = tokenStream.addAttribute(classOf[CharTermAttribute])
  tokenStream.reset()

  def terms:Stream[String]= Stream.cons( {tokenStream.incrementToken ;  new String(termAttr.buffer,0,termAttr.length) }, terms)
  println(terms.takeWhile(!_.isEmpty).toArray.mkString("=>"))

}
