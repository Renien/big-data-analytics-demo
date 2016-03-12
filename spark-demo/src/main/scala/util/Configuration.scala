package util

/**
 * Created by renienj on 2/26/16.
 */
object Configuration {
  val HDFS = "hdfs://sandbox.hortonworks.com:8020"

  val KEYS_FILE = "/user/guest/iot-demo/keys"

  val FEED_FILE = "/user/guest/iot-demo/data/sample_fb_data.feed"
  val KVS_FILE = "/user/guest/iot-demo/kvs"

  val KEY_FIELD = "user_id"
  val FEED_MAPPED_FILE = "/user/guest/iot-demo/data/fb_mappers"

  val FEED_GRAPH = "/user/guest/iot-demo/graph-data/"
  val FB_FEED_KEY_VALUE_CSV = FEED_GRAPH.concat("sample_fb_data_csv/")
  val FB_FEED_CSV_FILE = FEED_GRAPH.concat("sample_fb_data.csv")

  val FB_FEED_HBASE_NAME = "fb_feed"
  val HIVE_QUERY_PATH= FEED_GRAPH.concat("sample_fb_data.sql")

}

