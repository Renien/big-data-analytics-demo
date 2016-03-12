package mappers

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
 * Created by renienj on 2/26/16.
 */
class FBFeedFileFormat extends FileInputFormat[Text, Seq[(Text, Text)]]{
  override def createRecordReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): RecordReader[Text, Seq[(Text, Text)]] = {
    new FBFeedRecordReader()
  }
}