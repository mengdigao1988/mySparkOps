package cn.mengdi.spark.util

import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag


object TopK {

  // wrap that contains top k elems
  case class Tops[T <: Ordered[T] : Encoder](K: Int, var tops: Array[T]) {

    // + method used for agg
    def +(that: Tops[T])(implicit ct: ClassTag[T]): Tops[T] = {

      // add elements from that to this.tops
      for (elem <- that.tops) {

        if (this.tops.size < K) {
          // add that elem to tops if tops size is less than k
          this.tops = add(this.tops, elem)

          // build heap when num of elems reaches k
          if (this.tops.size == K) {
            this.tops = buildHeap(this.tops)
          }
        } else {
          // insert new elem to heap, when heap is built
          insert(this.tops, elem)
        }
      }
      this
    }
  }

  // add elem to current tops if the length of tops is smaller than k
  private def add[T](`this`: Array[T], that: T)(implicit ct : ClassTag[T]): Array[T] = {
    val newArr = `this` :+ that
    newArr
  }

  // insert elem to heap
  private def insert[T <: Ordered[T]](`this`: Array[T], that: T): Unit = {
    MinHeap.insert(that, `this`)
  }


  /* build min heap with first k elements*/
  private def buildHeap[T <: Ordered[T]](data: Array[T]): Array[T] = {
    MinHeap.buildHeap(data)
  }


}
