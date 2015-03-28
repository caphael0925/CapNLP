
/**
 * Created by caphael on 15/3/29.
 */
object IndependenceTest extends App{

  import com.caphael.nlp.word.WordMetricUtils
  import com.caphael.nlp.util.SplitUtils
  import com.caphael.nlp.word.{WordMetricUtils => wm}
  import org.apache.spark.{SparkContext, SparkConf}

  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)
  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"

  val inputRaw = sc.textFile(inputp).map(_.split(",",2)(1))
  val input = wm.flatten(inputRaw,SplitUtils.sentenceSplit).distinct

  val res = WordMetricUtils.getMetrics(input,-1,2)

}
