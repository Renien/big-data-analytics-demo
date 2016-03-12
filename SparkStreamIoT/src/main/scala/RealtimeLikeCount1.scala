import java.io.{OutputStreamWriter, BufferedWriter}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 */
object RealtimeLikeCount1 extends App {

  /**
   * Profiles
   */
  val profiles = Map(
  "sam" -> ("M", 27),
  "ann" -> ("F", 19),
  "dave" -> ("M", 43),
  "lin" -> ("F", 34),
  "dray" -> ("M", 13)
  )

  /**
   * The summery of all the likes based on gender
   */
  val likes_summery = mutable.Map[String, Int]().withDefaultValue(0)

  val likePattern = "(\\w+) likes (\\w+) s (\\w+)".r

  val conf = new SparkConf().setAppName("RealtimeLikeCount1").setMaster("local[2]")

  val ssc = new StreamingContext(conf, Seconds(1))

  /**
   * The likes stream
   */
  val likes = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)

  /**
   * Likes based on gender
   */
  val gender_likes = likes.map {
    case likePattern(a, b, c) => for {
      a1 <- profiles.get(a)
      b1 <- profiles.get(b)
    } yield {
        (a1._1+b1._1, 1)
      }
    case x => None
  } filter(_.isDefined) map(_.get)

  /**
   * A collected summery for DStream RDD
   */
  val gender_likes_collected = gender_likes reduceByKey(_ + _)

  /**
   * Update summery and stream out the report
   */
  gender_likes_collected.foreachRDD(_.foreachPartition{p =>
    val connection = new Socket("localhost", 9998)
    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))
    p.foreach{l =>
      likes_summery(l._1) += l._2
      bufferedWriter.write(s"Event: ${l._1} => ${l._2}\n")
      bufferedWriter.write(s"Summery: ${likes_summery.toSeq.map(e=>s"${e._1} is ${e._2}").mkString(" ")}\n\n")
    }
    bufferedWriter.close()
    connection.close();
  })

  ssc.start()
  ssc.awaitTermination()
}
