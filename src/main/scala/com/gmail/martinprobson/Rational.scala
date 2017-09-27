package com.gmail.martinprobson



//TODO - Override hashcode

class Rational(n: Int, d: Int) {
  
  require(d != 0)
  
  
  private val g = Rational.gcd(n.abs,d.abs)
  val numer: Int = n / g
  val denom: Int = d / g
  
  def this(n: Int) = this(n,1)
  
  def +(r: Rational) = new Rational(denom * r.numer + numer * r.denom,denom * r.denom)
  def +(i: Int) = new Rational(numer + i * denom, denom)
  
  def -(r: Rational) = new Rational(numer * r.denom - r.numer * denom, denom * r.denom)
  def -(i: Int) = new Rational(numer - i * denom, denom)
  
  def *(r: Rational) = new Rational(numer * r.numer, denom * r.denom)
  def *(i: Int) = new Rational(numer * i, denom)
  
  def /(r: Rational) = new Rational(numer * r.denom, denom *r.numer)
  def /(i: Int) = new Rational(numer,denom * i) 
  
//  private def gcd(a: Int,b: Int): Int = if (b == 0) a else gcd(b, a % b)
  
  override def toString = numer + " / " + denom
  
  override def equals(other: Any) = other match {
    case that: Rational => (that canEqual this) && 
                           this.numer == that.numer &&
                           this.denom == that.denom
    case _ => 
        false
  }
  
  def canEqual(other: Any) = other.isInstanceOf[Rational]

}

object Rational {
  def apply(n: Int, d: Int) = new Rational(n,d)
  def apply(n: Int) = new Rational(1)

  private def gcd(a: Int,b: Int): Int = if (b == 0) a else gcd(b, a % b)
}
