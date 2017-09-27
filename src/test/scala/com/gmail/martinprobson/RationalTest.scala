package com.gmail.martinprobson

import org.junit._
import Assert._

@Test
class AppTest {

    @Test
    def testRationalEquals() = {
          assertTrue("Rational(1,2) == Rational(1,2)",Rational(1,2) == Rational(1,2))
          assertTrue("Rational(2,4) == Rational(1,2)",Rational(2,4) == Rational(1,2))
          assertTrue("Rational(1) == Rational(1,1)",Rational(1) == Rational(1,1))
          assertTrue("Rational(100,1) == Rational(200,2)",Rational(100,1) == Rational(200,2))

          assertFalse("Rational(1,4) == Rational(1,2)",Rational(1,4) == Rational(1,2))
          assertFalse("Rational(6,10) == Rational(1,2)",Rational(6,10) == Rational(1,2))
          assertFalse("Rational(1) == Rational(1,100)",Rational(1) == Rational(1,100))

    }
    @Test
    def testRational() = {
          val actual = List( Rational(1,2),
                          Rational(2,4),
                          Rational(1,2) + Rational(2,4),
                          Rational(1,2) * Rational(2,4),
                          Rational(56,67),
                          Rational(3,8),
                          Rational(3,10),
                          Rational(3,8) + Rational(3,10),
                          Rational(3,8) * Rational(3,10),
                          Rational(3,8) * Rational(3,10) / Rational(1,2),
                          Rational(3,10) * 2,
                          Rational(3,8) * Rational(3,10) / Rational(1,2) * 10)
          val expected = List( Rational(1,2),
                          Rational(1,2),
                          Rational(1),
                          Rational(1,4),
                          Rational(56,67),
                          Rational(3,8),
                          Rational(3,10),
                          Rational(27,40),
                          Rational(9,80),
                          Rational(9,40),
                          Rational(3,5),
                          Rational(9,4))
          val tests = expected zip actual
          tests foreach((pair) => assertTrue("Test: " + pair._1 + " == " + pair._2,pair._1 == pair._2))                          

    }


}


