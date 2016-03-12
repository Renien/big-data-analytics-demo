package mappers


import util.{Configuration, SparkJob}
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by renienj on 2/26/16.
 */
object FBFeedLoadJob extends SparkJob{
  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[OptionsConfig]("genkvs.mappers.EndecaSparkDataLoad") {
      head("Mappers")
      opt[String]('h', "hdfs") action {(x, c)=> c.copy(hdfs = x)} text "HDFS url"
      opt[String]('f', "feed file") action {(x, c)=> c.copy(feedFile = x)} text "Feed Input Filename"
      opt[String]('o', "out") action {(x, c)=> c.copy(out = x)} text "Feed file mapped output Filename"
      opt[String]('k', "KeyField") action {(x, c)=> c.copy(key = x)} text "Key Field"
    }
    val options: OptionsConfig = parser.parse(args, OptionsConfig()).get

    val config = new SparkConf().setAppName("FBFeedLoadJob")
    val sc = new SparkContext(config)

    val conf = new org.apache.hadoop.conf.Configuration(sc.hadoopConfiguration)
    conf.set("KeyField", s"${options.key}")

    val fbFeedFile = s"${options.hdfs}${options.feedFile}"
    val fbFeedFormatterdObj = s"${options.hdfs}${options.out}"

    val input = sc.newAPIHadoopFile(fbFeedFile, classOf[FBFeedFileFormat], classOf[Text], classOf[Seq[(Text, Text)]], conf)
    input.filter(_._1 != "empty").map{ x=>
      (x._1.toString,
        x._2.map{ y=>
          (y._1.toString, y._2.toString)
        })
    }.saveAsObjectFile(fbFeedFormatterdObj)
  }

  case class OptionsConfig(
                            hdfs: String = Configuration.HDFS,
                            feedFile: String = Configuration.FEED_FILE,
                            out: String = Configuration.FEED_MAPPED_FILE,
                            key: String = Configuration.KEY_FIELD
                            )
}