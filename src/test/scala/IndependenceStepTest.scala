import com.caphael.nlp.metric.MetricMap
import com.caphael.nlp.word.TermMetricNode

/**
* Created by caphael on 15/3/25.
*/


object IndependenceStepTest extends App{

  import com.caphael.nlp.util.SplitUtils
  import com.caphael.nlp.metric.MetricUtils._
  import com.caphael.nlp.hadoop.HDFSHandler
  import com.caphael.nlp.word.WordMetricUtils._
  import com.caphael.nlp.word.TermMetric
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkContext, SparkConf}
  import com.caphael.nlp.word.TermMetricNode
  import com.caphael.nlp.metric.MetricType._


  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)

//  System.setProperty("spark.master","local")

  //Control parameters
  val recalcTermProb = true
  val recalcTermSeqMetricsUnion = true
  //===============================

  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"
  val outputp=inputp+".out"
  val termSeqProbsUnion_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermSeqProbsUnion"
  val termProb_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermProb"

  val charCountL = 0L
  val maxWordLen=2
  val minWordFreq=5
  val relevance_delta=10
  val entropy_delta=1

  val hdfs = HDFSHandler(inputp)

  val inputRaw = sc.textFile(inputp)
  val input = flatten(inputRaw,SplitUtils.sentenceSplit).distinct

  val charCount:Long = if(charCountL>0L) {
    charCountL
  } else {
    input.map(_.length).reduce(_+_)
  }

  //

  //Predeal
  val inputPredealed = predeal(input);

  //Term splitting
  val unFlatTerms:RDD[Array[String]] = inputPredealed.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty)
  val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x)

  //Initialize Single Term Metric Dictionary
  val singleTermFreq:RDD[TermMetric] = getFrequenciesByString(flatTerms)
  val singleTermProb:RDD[TermMetric] = getProbabilities(singleTermFreq,charCount)
  ////Generate the term-prob dictionary(1 char)
  val singleTermMetricMap = singleTermProb.map(x=>(x.ID,x)).collect.toMap

  //Initialize Terms into TermMetrics(With Sliding Grouping)
  ////Function that Initialize Array[String] to Array[TermMetricNode](which contained neighbourhood information)
  val initTermMetricsFun:(Array[String])=>Array[TermMetricNode] = initTermMetricsProtype(singleTermMetricMap) _
  ////Array[String] => Array[TermMetricNode]
  val initTermMetricNodes:RDD[Array[TermMetricNode]] = unFlatTerms.map(initTermMetricsFun(_))
  ////Merge two neighbouring TermMetricNodes into one with Sliding Window(length=2)
  val termMetricNodesSliding:RDD[TermMetricNode] = initTermMetricNodes.filter(_.length>1).flatMap(neighbourSplit(_))

  //Initialize Neighbouring Terms Metric Dictionary
  val neighbourTermFreq:RDD[TermMetric] = getFrequenciesByTermMetric(termMetricNodesSliding.map(_.CORE)).filter(x=>x.METRICS(Frequency)>=minWordFreq)
  val neighbourTermProb:RDD[TermMetric] = getProbabilities(neighbourTermFreq,charCount)

  //Calculate Relevance between Neighbouring Terms
  val neighbourTermRele:RDD[TermMetric] = getRelevance(neighbourTermProb).filter(x=>x.METRICS(Relevance)>relevance_delta)

  val neighbourTermEtpy:RDD[TermMetric] = getEntropy(termMetricNodesSliding).
    filter(x=>x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta)

  val neighbourTermMetric:RDD[TermMetric] = neighbourTermEtpy.map(x=>(x.ID,x)).join(neighbourTermRele.map(x=>(x.ID,x))).
    map{
    case(id,ms) => ms._1.METRICS++=ms._2.METRICS ; ms._1
  }


//  val neighbourTermMetricMap = neighbourTermRele.map(x=>(x.ID,x)).collect().toMap


//
//  res.saveAsObjectFile(outputp)

}
