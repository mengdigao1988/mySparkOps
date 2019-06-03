package cn.mengdi.spark.util

import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

object BottomK {

  case class Bottoms[T <: Ordered[T] : Encoder](k : Int, var bottoms: Array[T]) {

    def +(that: Bottoms[T])(implicit ct: ClassTag[T]): Bottoms[T] = {

      for (elem <- that.bottoms) {

        if (this.bottoms.size < k) {
          // add that elem to tops if tops size is less than k
          this.bottoms = add(this.bottoms, elem)

          // build heap if num of datas reaches k
          if (this.bottoms.size == k) {
            this.bottoms = buildHeap(this.bottoms)
          }
        } else {
          // insert new elem to heap, when heap is build
          insert(this.bottoms, elem)
        }
      }
      this
    }
  }

  // add elem to bottoms before size of bottoms reaches k
  private def add[T](`this`: Array[T], that: T)(implicit ct: ClassTag[T]): Array[T] = {
    val newArr = `this` :+ that
    newArr
  }

  // insert elem to heap
  private def insert[T <: Ordered[T]](`this`: Array[T], that: T): Unit  = {
    MaxHeap.insert(that, `this`)
  }

  // build min heap with first k elements
  private def buildHeap[T <: Ordered[T]](data: Array[T]): Array[T] = {
    MaxHeap.buildHeap(data)
  }
}
