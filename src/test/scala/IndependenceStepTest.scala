import com.caphael.nlp.metric.MetricType._
import com.caphael.nlp.util.MetricUtils._
import com.caphael.nlp.util.{HadoopUtils, SplitUtils}
import com.caphael.nlp.word.{WordMetricUtils => wm, TermMetric}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
* Created by caphael on 15/3/25.
*/


object IndependenceStepTest extends App{

  import com.caphael.nlp.util.SplitUtils
  import com.caphael.nlp.word.{WordMetricUtils => wm}
  import org.apache.spark.{SparkContext, SparkConf}

  val conf = new SparkConf().setAppName("Test").setMaster("local")
  val sc = new SparkContext(conf)

//  System.setProperty("spark.master","local")

  //Control parameters
  val recalcTermProb = false
  val recalcTermSeqMetricsUnion = false
  //===============================

  val inputp="hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/input.txt"
  val outputp=inputp+".out"
  val termSeqProbsUnion_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermSeqProbsUnion"
  val termProb_path = "hdfs://Caphael-MBP:9000/user/caphael/SparkWorkspace/NLP/WordDiscover/TermProb"

  val sentencesCountL = 0L
  val maxWordLen=2
  val minWordFreq=2
  val delta=100

  val hdfs = HadoopUtils.hdfsHandler(inputp)

  val inputRaw = sc.textFile(inputp).map(_.split(",",2)(1))
  val input = wm.flatten(inputRaw,SplitUtils.sentenceSplit).distinct

  val sentencesCount:Long = if(sentencesCountL>0L) {
    sentencesCountL
  } else {
    input.count
  }

  //Term splitting
  val unFlatTerms:RDD[Array[String]] = input.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty).cache

  val termProb:RDD[TermMetric] = if(recalcTermProb){
    val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x.distinct)
    val flatInitTermMetrics:RDD[TermMetric] = flatTerms.map(x=>TermMetric(x,MetricMap()))
    val termFreq:RDD[TermMetric] = getFrequencies(flatInitTermMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
    val ret = getProbabilities(termFreq,sentencesCount)
    if(hdfs.exists(termProb_path)) hdfs.delete(termProb_path)
    ret.saveAsObjectFile(termProb_path)
    ret
  }else{
    sc.objectFile[TermMetric](termProb_path)
  }


  //Generate the term-prob dictionary
  val termProbDict = termProb.map(x=>(x.ID,x)).collect.toMap

  val termSeqProbsUnion = if(recalcTermSeqMetricsUnion){
    //Calc the term-sequences frequencies
    val termSeqProbs = for(i<-2 to maxWordLen) yield {

      //Initialize TermSeqMetric by
      val initTermSeqMetrics:RDD[TermMetric] = unFlatTerms.map{
        case(x) => TermMetric(x.mkString
          ,MetricMap()
          ,x.map(termProbDict.getOrElse(_,TermMetric.NULL)))
      }.filter(x=>x.SUBTERMS.forall(_!=TermMetric.NULL))

      val flatTermSeqMetrics:RDD[TermMetric] = initTermSeqMetrics.flatMap(SplitUtils.neighbourSplit(i,true)(_))
      val termSeqFreq:RDD[TermMetric] = getFrequencies(flatTermSeqMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
      val termSeqProb:RDD[TermMetric] = getProbabilities(termSeqFreq,sentencesCount)
      termSeqProb
    }
      //Union all the word probabilities
    val ret = termSeqProbs.reduceLeft(_.union(_))

    if(hdfs.exists(termSeqProbsUnion_path)) hdfs.delete(termSeqProbsUnion_path)
    ret.saveAsObjectFile(termSeqProbsUnion_path)
    ret
  }else{
    sc.objectFile[TermMetric](termSeqProbsUnion_path)
  }

  //Calculate independence rank of each character sequence and filter the entry above delta
  val res = getIndependence(termSeqProbsUnion).filter(x=>x.METRICS(Independence)>=delta)


//  val res = wm.getMetrics(inputDealed)

  res.saveAsObjectFile(outputp)

}
