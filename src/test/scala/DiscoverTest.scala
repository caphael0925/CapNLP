
/**
* Created by caphael on 15/3/29.
*/
object DiscoverTest extends App{

  import com.caphael.nlp.util.SplitUtils
  import com.caphael.nlp.word.WordMetricUtils._
  import org.apache.spark.{SparkContext, SparkConf}

  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)

  val inputp="SparkWorkspace/TextMining/WordDiscovery/sign.input"
  val outputp=inputp+".out"

  val inputRaw = sc.textFile(inputp).map(_.split(",",2)(1))
  val input = flatten(inputRaw,SplitUtils.sentenceSplit).distinct.repartition(20)

  val res = discover(input)

}
