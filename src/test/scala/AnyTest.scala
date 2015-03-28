//import com.caphael.nlp.word.{WordMetricUtils => wm}
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
// * Created by caphael on 15/3/25.
// */
//
//
//object AnyTest extends App{
//  val conf = new SparkConf().setAppName("Test").setMaster("local")
//  val sc = new SparkContext(conf)
//
////  System.setProperty("spark.master","local")
//
//  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"
//  val outputp=inputp+".out"
//
//
//  val input = sc.textFile(inputp).map(_.split(",",2)(1))
//  val inputDealed = wm.flatten(input,wm.SplitUtils.sentenceSplit)
//  val res = wm.getMetrics(inputDealed)
//
//
//  res.saveAsObjectFile(outputp)
//
//}
