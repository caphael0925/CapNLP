
/**
 * Created by caphael on 15/3/29.
 */
object DiscoverTest extends App{

  import com.caphael.nlp.util.SplitUtils
  import com.caphael.nlp.word.{WordMetricUtils => wm}
  import org.apache.spark.{SparkContext, SparkConf}

  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)

  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"
  val outputp=inputp+".out"

  val inputRaw = sc.textFile(inputp)
  val input = wm.flatten(inputRaw,SplitUtils.sentenceSplit).distinct.repartition(20)

  val res = wm.getMetrics(input,-1,2,5,30,1)

}
