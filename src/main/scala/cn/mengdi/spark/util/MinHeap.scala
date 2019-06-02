package cn.mengdi.spark.util


object MinHeap {

  /*
   build the minHeap using the data set with size k,
    */
  def buildHeap[U <: Ordered[U]](datas: Array[U]): Array[U] = {
    //  iterate the roots
    for (i <- (0 until datas.length / 2).reverse) {
      heapify(i, datas)
    }
    datas
  }
  
  /*
  insert elem to the heap
   */
  def insert[U <: Ordered[U]](elem: U, heap: Array[U]): Unit = {
    // return immediately if inserted data is even smaller than the root elem in the heap
    if (elem <= heap(0)) {
      return
    }

    // replace the root with inserted data who is greater, and heapify the heap
    heap(0) = elem
    heapify(0, heap)
  }


  // find the smallest element among root, left, right, and replace the root with smallest if needed
  private def heapify[U <: Ordered[U]](i: Int, datas: Array[U]): Unit = {

    val l = left(i)
    val r = right(i)

    // assume the smallest is the root
    var smallest = i

    // left node exists and smaller than root
    if (l < datas.length && datas(l) < datas(i)) {
      smallest = l
    }

    // right node exists and smaller than `smallest`
    if (r < datas.length && datas(r) < datas(smallest)) {
      smallest = r
    }

    // both left and right are greater than root
    if (i == smallest) {
      return
    }

    // else switch
    swap(i, smallest, datas)

    // heapify the heap since the smallest elem has changed
    heapify(smallest, datas)
  }

  // return the left child index
  private def left(i: Int): Int = {
    (i + 1) * 2
  }

  // return the right child index
  private def right(i: Int): Int = {
    (i + 1) * 2 - 1
  }

  // switch the elems at i and j
  private def swap[U<: Ordered[U]](i: Int, j: Int, datas: Array[U]): Unit = {
    val tmp = datas(i)
    datas(i) = datas(j)
    datas(j) = tmp
  }
}
