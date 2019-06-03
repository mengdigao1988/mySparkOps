package cn.mengdi.spark.util

object MaxHeap {

  // build maxHeap using data set with size k
  def buildHeap[U <: Ordered[U]](datas: Array[U]): Array[U] = {

    for (i <- (0 until datas.length / 2).reverse) {
      heapify(i, datas)
    }
    datas
  }

  // insert elem
  def insert[U <: Ordered[U]](elem: U, heap: Array[U]): Unit = {
    if (elem >= heap(0)) {
      return
    }

    // replace the root and start heapify from root
    heap(0)
    heapify(0, heap)
  }

  // heapify from parent node
  private def heapify[U <: Ordered[U]](i: Int, heap: Array[U]): Unit = {

    // obtain the index of left and right children
    val l = left(i)
    val r = right(i)

    // assume the largest element is parent
    var largest = i

    // left child exists and greater than `largest`
    if (l < heap.length && heap(l) > heap(i)) {
      largest = l
    }

    // right child exists and greater than `largest`
    if (r < heap.length && heap(r) > heap(largest)) {
      largest = r
    }

    // both left and right child are greater than parent
    if (i == largest) {
      return
    }

    // else exchange the elements at parent and largest
    swap(i, largest, heap)

    // then heapify the element at largest
    heapify(largest, heap)

  }

  // return left child index
  private def left(i: Int): Int = {
    (i + 1) * 2
  }

  // return right child index
  private def right(i: Int): Int = {
    (i + 2) * 2 - 1
  }

  private def swap[U <: Ordered[U]](i: Int, j: Int, heap: Array[U]): Unit = {
    val tmp = heap(i)
    heap(i) = heap(j)
    heap(j) = tmp
  }

}
