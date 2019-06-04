package cn.mengdi.spark.util

import cn.mengdi.spark.util.BottomK.Bottoms
import cn.mengdi.spark.util.TopK.Tops
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import cn.mengdi.spark.util.myImplicits._

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
      .map(t => Tops(k, 0, Array(t)))
      .groupByKey(t => func.apply(t.tops(0)))
      .reduceGroups((_: Tops[U]) + (_: Tops[U]))
      .flatMap(t => {
        t._2.tops
      })

  }


  def bottomkByKey[K : Encoder](k: Int, func: U => K)(implicit ct: ClassTag[U]): Dataset[U] = {

    data
      .map(t => Bottoms(k, Array(t)))
      .groupByKey(t => func.apply(t.bottoms(0)))
      .reduceGroups((_: Bottoms[U]) + (_: Bottoms[U]))
      .flatMap(t => {
        t._2.bottoms
      })
  }
}
