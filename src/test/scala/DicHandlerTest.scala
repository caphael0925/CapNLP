
/**
 * Created by caphael on 15/3/30.
 */
object DicHandlerTest extends App{
  import com.caphael.nlp.dictlib.DicHandler
  import com.caphael.nlp.dictlib.{CharCharParsing, LoadToMap, ReadFromFile}

  val path="library/common"
  val hdl= new DicHandler with ReadFromFile[String,String] with LoadToMap[String,String] with CharCharParsing
  val map = hdl.getDic(path)

  hdl.log("Finish")

}
