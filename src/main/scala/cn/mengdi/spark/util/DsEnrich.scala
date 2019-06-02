package cn.mengdi.spark.util

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import cn.mengdi.spark.util.myImplicits._

import scala.reflect.ClassTag

class DsEnrich[U <: Ordered[U] : Encoder](data: Dataset[U], spark: SparkSession) extends Serializable {

  /**
    * the method that return top K elements within each key,
    * min heap algorithm is used within the operator,
    * and heap is build at map-side of shuffle, which reduces the
    * the amount of data during shuffle write
    * @param k top k
    * @return
    */
  def topkByKey[K : Encoder](k: Int, func: U => K)(implicit ct: ClassTag[U]): Dataset[U] = {

    data
      .map(t => TopK.build(k, t))
      .groupByKey(t => func.apply(t.tops(0)))
      .reduceGroups(_ + _)
      .flatMap(t => {
        val tops = t._2.tops
        tops
      })
  }
}
