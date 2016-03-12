package util

/**
 * Created by renienj on 2/26/16.
 *
 * Marker interface for a testable spark job.
 * This separates RDD IO from actual processing thus allowing to write unit
 * tests on process
 *
 * @since 1.0
 */
trait SparkJob extends Serializable {
}
