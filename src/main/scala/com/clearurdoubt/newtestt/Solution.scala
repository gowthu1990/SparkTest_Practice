package com.clearurdoubt.newtestt

import scala.annotation.tailrec

object Solution {

  def getNextNumber(n: Int): Int = {
    n.toString.map(ch => {
      val x = Integer.valueOf(ch.toString)
      x * x
    }).sum
  }


  @tailrec
  def process(init: Int, acc: List[Int]): (Int, Boolean) = {
    if(init == 1) (0, true)
    else {
      val out = getNextNumber(init)

      if(acc.contains(init)) {
        val index = acc.indexOf(init)
        (index + 1, false)
      }
      else process(out, init :: acc)
    }
  }
}

object SolutionDemo {
  def main(args: Array[String]): Unit = {
    val in = 14
    val (index, outcome) = Solution.process(in, List())
    if(outcome) println(s"$in is a Happy Number.")
    else println(s"$in is not a Happy Number and consists of $index repeating elements")
  }
}
