package docdb

import java.net.URI


import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import util.{Configuration, SparkJob, Helper}

/**
 * Created by renienj on 2/27/16.
 */
object CreateCSVFiles extends SparkJob {

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[OptionsConfig]("genkvs.docdb.CreateCSVFiles") {
      head("docdb")
      opt[String]('h', "hdfs") action {(x, c)=> c.copy(hdfs = x)} text "HDFS url"
      opt[String]('i', "input ") action {(x, c)=> c.copy(in = x)} text "Input Filename"
      opt[String]('o', "out") action {(x, c)=> c.copy(out = x)} text "Output Filename"
      opt[String]('c', "csv") action {(x, c)=> c.copy(csv = x)} text "csv Filename and location"
      opt[String]('e', "exe") action {(x, c)=> c.copy(exe = x)} text "execute both or hive or csv"
      opt[String]('k', "key") action {(x, c)=> c.copy(key = x)} text "key field for endeca"
      opt[String]('b', "hbaseName") action {(x, c)=> c.copy(hbaseName = x)} text "HBase name"
      opt[String]('q', "hiveQueryPath") action {(x, c)=> c.copy(hiveQueryPath = x)} text "hiveQueryPath location"
    }
    val options: OptionsConfig = parser.parse(args, OptionsConfig()).get

    val config = new SparkConf().setAppName("CreateCSVFiles")
    val sc = new SparkContext(config)

    val uri = s"${options.hdfs}"
    val execution = s"${options.exe}"

    val endecaFile = s"${options.hdfs}${options.in}"
    val csvParticalFiles = s"${options.hdfs}${options.out}"
    val csvFile = s"${options.hdfs}${options.csv}"

    val hBaseName = s"${options.hbaseName}"
    val key = s"${options.key}"
    val hiveQueryPath = s"${options.hdfs}${options.hiveQueryPath}"

    val input = sc.objectFile[(String, Seq[(String, String)])](endecaFile)
    val headers = headingColumn(input).collect()

    if(execution == "both" || execution == "csv") {
      //Make Pipe separated csv file
      makePipeSeparateRow(input, headers)
        .saveAsTextFile(csvParticalFiles) //Save the partial page
      merge(sc, csvParticalFiles, csvFile, headers.mkString("|")) //Write to csv
    }

    if(execution == "both" || execution == "hive") {
      //Write to hive query file
      val query = generateHiveBulkUploadQuery(headers,hBaseName, key, csvFile)
      write(sc,uri,hiveQueryPath,query.getBytes)
    }

  }

  //Merage and create the CSV file
  def merge(sc: SparkContext,
            srcPath: String,
            dstPath: String, headers: String): Unit = {
    val srcFileSystem = FileSystem.get(new URI(srcPath),
      sc.hadoopConfiguration)
    val dstFileSystem = FileSystem.get(new URI(dstPath),
      sc.hadoopConfiguration)
    dstFileSystem.delete(new Path(dstPath), true)
    Helper.copyMergeWithHeader(srcFileSystem,  new Path(srcPath),
      dstFileSystem,  new Path(dstPath),
      false, sc.hadoopConfiguration, headers)
  }

  def makePipeSeparateRow(in : RDD[(String, Seq[(String, String)])], column: Seq[String]): RDD[String] = {

    //Group to one column all the same key type
    val modelList = in.map(x => (x._1, x._2.groupBy(s => s._1).mapValues(z=>{
      if(z.size > 1)
        z.map(_._2).mkString(",")
      else
        z.map(_._2).mkString("")
    })))

    //Create the rows
    modelList.map{ x=>
      column.map(k=>x._2.find(_._1 == k).getOrElse((k,""))._2).mkString("|")
    }
  }

  //Get the unique column names
  def headingColumn(in : RDD[(String, Seq[(String, String)])]) = {
    in.flatMap{ x=>
      x._2.map{ y=>
        y._1
      }
    }.distinct
  }

  def generateHiveBulkUploadQuery(headers: Array[String], hBaseName: String, key: String,
                                  fileLocation: String): String = {

    //Drop Hbase
    val dropHbase = s"DROP TABLE IF EXISTS ${hBaseName};"

    //Create HBase tables
    val query = s"CREATE TABLE IF NOT EXISTS ${hBaseName}(${headers.map(_.concat(" STRING")).mkString(",")})\n" +
      "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
      s"WITH SERDEPROPERTIES ('hbase.columns.mapping' = '${headers.map(h=>{
        if (key == h)
          ":key"
        else
          hBaseName.concat(s"_data:$h")
      }).mkString(",")}')\n" +
      s"TBLPROPERTIES ('hbase.table.name' = '${hBaseName.concat("_snap_shot")}');"

    //Create source table
    val hbaseSource = s"CREATE TABLE IF NOT EXISTS ${hBaseName.concat("_source_table")}(${headers.map(_.concat(" STRING")).mkString(",")})\n" +
      s"ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;\n\n" +
      s"LOAD DATA INPATH '$fileLocation' INTO TABLE ${hBaseName.concat("_source_table")};"

    //Insert to Hbase table
    val insertHbase = s"FROM ${hBaseName.concat("_source_table")} INSERT INTO TABLE ${hBaseName}\n" +
      s"SELECT ${hBaseName.concat("_source_table")}.* WHERE ${key} != '';"

    val dropHbaseSource = s"DROP TABLE IF EXISTS ${hBaseName.concat("_source_table")};"

    val fullQuery = dropHbase.concat("\n\n")
      .concat(query).concat("\n\n")
      .concat(hbaseSource).concat("\n\n")
      .concat(insertHbase).concat("\n\n")
      .concat(dropHbaseSource)

    fullQuery

  }

  def write(sc: SparkContext, uri: String,filePath: String, data: Array[Byte]): Unit = {
    val srcFileSystem = FileSystem.get(new URI(uri),
      sc.hadoopConfiguration)
    val os = srcFileSystem.create(new Path(filePath))
    os.write(data)
    srcFileSystem.close()
  }

  case class OptionsConfig(
                            hdfs: String = Configuration.HDFS,
                            in: String = Configuration.FEED_MAPPED_FILE,
                            out: String = Configuration.FB_FEED_KEY_VALUE_CSV,
                            csv: String = Configuration.FB_FEED_CSV_FILE,
                            exe: String = "both",
                            key: String = Configuration.KEY_FIELD,
                            hbaseName: String = Configuration.FB_FEED_HBASE_NAME,
                            hiveQueryPath: String = Configuration.HIVE_QUERY_PATH
                            )

}