//
//
///**
//* Created by caphael on 15/3/25.
//*/
//
//
//object IndependenceStepTestOnline extends App{
//
//  import com.caphael.nlp.dictlib.CharCharDicHandler
//  import com.caphael.nlp.hadoop.HDFSHandler
//  import com.caphael.nlp.metric.MetricType._
//  import com.caphael.nlp.predeal.PredealUtils
//  import com.caphael.nlp.metric.MetricUtils._
//  import com.caphael.nlp.util.SplitUtils
//  import com.caphael.nlp.word.{TermMetric, WordMetricUtils => wm}
//  import org.apache.spark.rdd.RDD
//
//
//  import org.apache.spark.{SparkConf, SparkContext}
//
//  val conf = new SparkConf().setAppName("Test").setMaster("local")
//  val sc = new SparkContext(conf)
//
////  System.setProperty("spark.master","local")
//
//  //Control parameters
//  val recalcTermProb = true
//  val recalcTermSeqMetricsUnion = true
//  //===============================
//
//  val inputp="hdfs://namenode125:9000/user/root/Recommendation/CB/SignStats/WordDiscover/input"
//  val outputp="hdfs://namenode125:9000/user/root/Recommendation/CB/SignStats/WordDiscover/output"
//  val termSeqProbsUnion_path = "hdfs://namenode125:9000/user/root/Recommendation/CB/SignStats/WordDiscover/TermSeqProbsUnion"
//  val termProb_path = "hdfs://namenode125:9000/user/root/Recommendation/CB/SignStats/WordDiscover/TermProb"
//
//  val sentencesCountL = 0L
//  val maxWordLen=2
//  val minWordFreq=2
//  val delta=100
//
//  val hdfs = HDFSHandler(inputp)
//
//  val inputRaw = sc.textFile(inputp)
//  val input = wm.flatten(inputRaw,SplitUtils.sentenceSplit).distinct
//
//  val sentencesCount:Long = if(sentencesCountL>0L) {
//    sentencesCountL
//  } else {
//    input.count
//  }
//
//  //Predeal
//  val dicPath = "library/common"
//  val dic = (new CharCharDicHandler).getDic(dicPath)
//  val inputPredealed = input.map(PredealUtils.charDicReplace(dic)(_))
//
//  //Term splitting
//  val unFlatTerms:RDD[Array[String]] = inputPredealed.map(SplitUtils.Lucene.standardSplit(false)(_)).filter(!_.isEmpty).cache
//
//  val termProb:RDD[TermMetric] = if(recalcTermProb){
//    val flatTerms:RDD[String] = unFlatTerms.flatMap(x=>x.distinct)
//    val flatInitTermMetrics:RDD[TermMetric] = flatTerms.map(x=>TermMetric(x,MetricMap()))
//    val termFreq:RDD[TermMetric] = getFrequencies(flatInitTermMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
//    val ret = getProbabilities(termFreq,sentencesCount)
//    if(hdfs.exists(termProb_path)) hdfs.delete(termProb_path)
//    ret.saveAsObjectFile(termProb_path)
//    ret
//  }else{
//    sc.objectFile[TermMetric](termProb_path)
//  }
//
//
//  //Generate the term-prob dictionary
//  val termProbDict = termProb.map(x=>(x.ID,x)).collect.toMap
//
//  val termSeqProbsUnion = if(recalcTermSeqMetricsUnion){
//    //Calc the term-sequences frequencies
//    val termSeqProbs = for(i<-2 to maxWordLen) yield {
//
//      //Initialize TermSeqMetric by
//      val initTermSeqMetrics:RDD[TermMetric] = unFlatTerms.map{
//        case(x) => TermMetric(x.mkString
//          ,MetricMap()
//          ,x.map(termProbDict.getOrElse(_,TermMetric.TM_NULL)))
//      }.filter(x=>x.SUBTERMS.forall(_!=TermMetric.TM_NULL))
//
//      val flatTermSeqMetrics:RDD[TermMetric] = initTermSeqMetrics.flatMap(SplitUtils.neighbourSplit(i,true)(_))
//      val termSeqFreq:RDD[TermMetric] = getFrequencies(flatTermSeqMetrics).filter(_.METRICS(Frequency)>=minWordFreq)
//      val termSeqProb:RDD[TermMetric] = getProbabilities(termSeqFreq,sentencesCount)
//      termSeqProb
//    }
//      //Union all the word probabilities
//    val ret = termSeqProbs.reduceLeft(_.union(_))
//
//    if(hdfs.exists(termSeqProbsUnion_path)) hdfs.delete(termSeqProbsUnion_path)
//    ret.saveAsObjectFile(termSeqProbsUnion_path)
//    ret
//  }else{
//    sc.objectFile[TermMetric](termSeqProbsUnion_path)
//  }
//
//  //Calculate independence rank of each character sequence and filter the entry above delta
//  val res = getRelevance(termSeqProbsUnion).filter(x=>x.METRICS(Relevance)>=delta)
//
//
////  val res = wm.getMetrics(inputDealed)
//
//  res.saveAsObjectFile(outputp)
//
//}
