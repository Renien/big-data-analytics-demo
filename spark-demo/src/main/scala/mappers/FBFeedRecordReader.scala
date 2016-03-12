package mappers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.util.LineReader

/**
 * Created by renienj on 2/26/16.
 */
class FBFeedRecordReader  extends RecordReader[Text, Seq[(Text, Text)]]{

  private var start: Long = 0
  private var end: Long = 0
  private var pos: Long = 0
  private var lineReader: LineReader = null
  private var count = 0

  private val key = new Text()
  private var keyField: String = null
  private var value = Seq.empty[(Text, Text)]

  private val regex = """^([a-zA-Z-0-9_]+)\|\|(.+)\|L\|$""".r

  override def getProgress: Float = (pos-start)/(end - start)

  override def nextKeyValue(): Boolean = {
    val mb = Seq.newBuilder[(Text, Text)]
    if (pos >= end) {
      false
    } else {
      val found = true
      var loop = true
      while (loop){
        val text = new Text()
        pos += lineReader.readLine(text)
        text.toString match {
          case regex(a, b) =>
            mb.+=(new Text(a) -> new Text(b))
          case "|R||L|" =>
            value = mb.result()
            key.set(value.find(_._1.toString == keyField).getOrElse((new Text("empty"), new Text("empty")))._2)
            loop = false // Too bad scala is crappy when breaking loops
          //TODO: Handle corrupt files from creating infinite loops
          //            count +=1
          //            println(s"Found : ${count}")
          case _ =>
          //            println("^^^^^^^^^^^^^^ Some Error ^^^^^^^^^^^^^^")
          //            println(s"+++++++++ ACTUAL TEXT +++++++ ${text}")
          //found = false
        }
      }
      found
    }
  }

  override def getCurrentValue: Seq[(Text, Text)] = value

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {

    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    val job: Configuration = taskAttemptContext.getConfiguration

    start = fileSplit.getStart
    end = start + fileSplit.getLength

    val inputFile = fileSplit.getPath
    val fs = inputFile.getFileSystem(job)
    val fileIn = fs.open(inputFile)

    if (start != 0) start -= 7 // Provision for |R||L|\n
    fileIn.seek(start)
    println(s"Record Reader initializing from $start to $end")
    lineReader = new LineReader(fileIn)
    if(start != 0) {
      val text = new Text()
      do {
        start += lineReader.readLine(text) //.readLine(text, 0, Math.min(Int.MaxValue, end - start).toInt)
      } while (text.toString == "|R||L|" && start < end)
      pos = start
    }
    println(s"Record reader configured to read from $start to $end")
    keyField = job.get("endecaKeyField", "scode")
  }

  override def getCurrentKey: Text = key

  override def close(): Unit = {
    if (lineReader != null) {
      lineReader.close()
    }
  }
}