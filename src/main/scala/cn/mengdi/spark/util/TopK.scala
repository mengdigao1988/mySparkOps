package cn.mengdi.spark.util

import org.apache.spark.sql.Encoder

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object TopK {

  // wrap that contains top k elems
  case class Tops[T <: Ordered[T] : Encoder](K: Int, var heaped: Int, var tops: Array[T]) {
    def + (that: Tops[T])(implicit ct: ClassTag[T]): Tops[T] = {

      if (heaped == 1) {
        if (that.heaped == 1) {
          // both tops heapified
          merge(tops, that.tops)
          this
        } else {
          // only this.tops heapified
          insertAll(that.tops, tops)
          this
        }
      } else {
        if (that.heaped == 1) {
          // only that.tops heapified
          insertAll(tops, that.tops)
          that
        } else {
          // both tops not heapified
          for (t <- that.tops) {
            if (heaped == 1) {
              insert(tops, t)
            } else {
              // this check is to support the case that k == 1
              if (tops.length == K) {
                buildHeap(tops)
                insert(tops, t)
                heaped = 1
              } else {
                tops = add(tops, t)
              }
            }
          }
          this
        }
      }

    }
  }


  // add elem to current tops if the length of tops is smaller than k
  private def add[T](unheap: Array[T], elem: T)(implicit ct : ClassTag[T]): Array[T] = {
      unheap :+ elem
  }

  // insert elem to heap
  private def insertAll[T <: Ordered[T]](unheap: Array[T], heap: Array[T]): Unit = {
    MinHeap.insertAll(unheap, heap)
  }

  private def insert[T <: Ordered[T]](heap: Array[T], elem: T): Unit = {
    MinHeap.insert(elem, heap)
  }

  private def merge[T <: Ordered[T]](heap1: Array[T], heap2: Array[T]): Unit = {
    MinHeap.merge(heap1, heap2)
  }

  /* build min heap with first k elements*/
  private def buildHeap[T <: Ordered[T]](data: Array[T]): Array[T] = {
    MinHeap.buildHeap(data)
  }


}
