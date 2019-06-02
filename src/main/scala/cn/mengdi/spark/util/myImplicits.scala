package cn.mengdi.spark.util

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import scala.language.implicitConversions
import scala.reflect.ClassTag

object myImplicits {
  // implicitly convert classTag to Encoder
  implicit def kEncoder[A](implicit ct: ClassTag[A]):Encoder[A] = Encoders.kryo[A](ct)

  implicit def ds2dsEnrich[U <: Ordered[U] : Encoder](data: Dataset[U]): DsEnrich[U] =
    new DsEnrich[U](data, spark = data.sparkSession)
}
