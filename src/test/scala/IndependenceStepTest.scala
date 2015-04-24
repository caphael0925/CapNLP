import com.caphael.nlp.metric.MetricMap
import com.caphael.nlp.word.{TermNode, TermMetricNode}

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
  import com.caphael.nlp.word.TermNode
  import com.caphael.nlp.metric.MetricType._



//  System.setProperty("spark.master","local")

  //Control parameters
  val recalcTermProb = true
  val recalcTermSeqMetricsUnion = true
  //===============================


  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)

  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"
  val outputp=inputp+".out"
  val termSeqProbsUnion_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermSeqProbsUnion"
  val termProb_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermProb"

  val charCountL = 0L
  val maxWordLen=3
  val relevance_delta=20
  val entropy_delta=1.5

  val hdfs = HDFSHandler(inputp)

  val inputRaw = sc.textFile(inputp)
  val input = flatten(inputRaw,SplitUtils.sentenceSplit).distinct

  val charCount:Long = if(charCountL>0L) {
    charCountL
  } else {
    input.map(_.length).reduce(_+_)
  }

  val minWordFreq=(charCount/100000.0).ceil+1

  val partitionNum:Int = charCount.toInt/DEFAULT.PARTITION_SIZE
  val partInput = input.repartition(partitionNum)

  //Predeal
  val inputPredealed = predeal(partInput);

  //Term splitting
  val unFlatTerms:RDD[Array[String]] = inputPredealed.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty)
  val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x)

  //Initialize Single Term Metric Dictionary
  val singleTermFreq:RDD[TermMetric] = getFrequenciesByString(flatTerms)
  val singleTermProb:RDD[TermMetric] = getProbabilities(singleTermFreq,charCount)
  ////Generate the term-prob dictionary(1 char)
  //=================================Single Term Metric Dictionary!==========================================
  var singleTermMetricMap = singleTermProb.map(x=>(x.ID,x)).collect.toMap
  //=================================Yes it is!==============================================================

  //=================================Temp Multiple Terms Metric Dictionary!==================================
  var tempTermMetricMap =  singleTermMetricMap
  //=================================en en===================================================================

  //Initialize Terms into TermMetrics(With Sliding Grouping)
  ////Function that Initialize Array[String] to Array[TermMetricNode](which contained neighbourhood information)
  //如果把init步骤放在Sliding后面？那就要使用TermNode而不是TermMetricNode
  ////Array[String] => Array[TermNode]
  val initTermNodes:RDD[IndexedSeq[TermNode]] = unFlatTerms.map(initTermNodesFun(_))

  val res=for (curWordLen<-2 to maxWordLen) yield {
    //Function to get SubTermMetrics
    val getSubTermMetricsFun = getSubTermMetricsProtype(singleTermMetricMap,tempTermMetricMap) _

    ////Merge two neighbouring TermMetricNodes into one with Sliding Window(length=2,3...maxWordLen)
    val termNodesSliding:RDD[TermNode] = initTermNodes.filter(_.length>curWordLen).flatMap(SplitUtils.neighbourSplit(curWordLen,false)(_))

    //Calculate Frequency & Generate TermMetric Constructor (RDD[TermNode]=>RDD[TermMetric])
    val termMetricFreq:RDD[TermMetric] = getFrequenciesByTermNode(termNodesSliding).filter(x=>x(Frequency)>=minWordFreq)

    //Update TermMetric Objects with SubTerms (RDD[TermMetric]=>RDD[TermMetric])
    val termMetricFreqWithSubTerms=termMetricFreq.map(x => {x.SUBTERMS = getSubTermMetricsFun(x.ID);x}).filter(_.SUBTERMS!=TermMetric.ST_NULL)

    //Calculate Probability
    val termMetricProb:RDD[TermMetric] = getProbabilities(termMetricFreqWithSubTerms,charCount)

    //Calculate Relevance
    val termMetricRele:RDD[TermMetric] = getRelevance(termMetricProb).filter(x=>x(Relevance)>relevance_delta)

    //Calculate Entropy
    val termMetricEntr:RDD[TermMetric] = getEntropy(termNodesSliding)
    val termMetricEntr4Curr:RDD[TermMetric] = termMetricEntr.
      filter(x=>x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta)
    val termMetricEntr4Temp:RDD[TermMetric] = termMetricEntr.
      filter(x=> !(x.METRICS(LeftEntropy)>entropy_delta&&x.METRICS(RightEntropy)>entropy_delta))

    //Merge Metrics into one
    val currentResult:RDD[TermMetric] = termMetricRele.map(x=>(x.ID,x)).join(termMetricEntr4Curr.map(x=>(x.ID,x))).
      map{
      case(id,ms) => ms._1.METRICS++=ms._2.METRICS ; ms._1
    }

    //Fresh the TempTermMetricMap
    tempTermMetricMap = termMetricProb.map(x=>(x,null)).join(termMetricEntr4Temp.map(x=>(x,null))).map{
      case (tm,ns)=> (tm.ID,tm)
    }.collect.toMap

    currentResult
  }

  res.reduce(_.union(_))

//  val neighbourTermMetricMap = neighbourTermRele.map(x=>(x.ID,x)).collect().toMap


//
//  res.saveAsObjectFile(outputp)

}
