package com.neo4j.bench.micro.data

import java.util

import com.neo4j.bench.micro.data.DiscreteGenerator.Bucket
import com.neo4j.bench.micro.data.TypeParamValues.{DBL, LNG, STR_BIG, STR_SML}
import com.neo4j.bench.micro.benchmarks.RNGState
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._
import com.neo4j.bench.micro.data.ConstantGenerator.constant;
import com.neo4j.bench.micro.data.DiscreteGenerator.discrete;
import scala.collection.mutable

object TypeParamValues {
  final val INT = "int"
  final val LNG = "long"
  final val FLT = "float"
  final val DBL = "double"
  final val STR_SML = "small_string"
  final val STR_BIG = "big_string"
  final val INT_ARR = "int[]"
  final val LNG_ARR = "long[]"
  final val FLT_ARR = "float[]"
  final val DBL_ARR = "double[]"
  final val STR_SML_ARR = "small_string[]"
  final val STR_BIG_ARR = "big_string[]"

  def randomListOf(valueType: String, valueCount: Int, distinctCount: Int): util.List[_] = {
    val rng = RNGState.newRandom(42)
    val valuesFun = valuesFunFor(valueType, distinctCount)
    // wrap in ArrayList because whatever asJava returns does not support shuffling
    valueType match {
      case LNG =>
        new util.ArrayList[Long](List.range(0, valueCount).map(_ => valuesFun.next(rng).asInstanceOf[Long]).asJava)
      case DBL =>
        new util.ArrayList[Double](List.range(0, valueCount).map(_ => valuesFun.next(rng).asInstanceOf[Double]).asJava)
      case STR_SML =>
        new util.ArrayList[String](List.range(0, valueCount).map(_ => valuesFun.next(rng).asInstanceOf[String]).asJava)
      case _ => throw new IllegalArgumentException(s"Invalid type: $valueType")
    }
  }

  private def valuesFunFor(valueType: String, distinctCount: Int): ValueGeneratorFun[_] = {
    val buckets = List.range(0, distinctCount).map(value => new Bucket(1, constant(valueType, value)))
    discrete(buckets: _*).create()
  }

  def listOf(valueType: String, valueCount: Int): util.List[_] = {
    val toObj: (Int) => Any = valueType match {
      case LNG => i => i.toLong
      case DBL => i => i.toDouble
      case STR_SML => i => i.toString
      case _ => _ => throw new IllegalArgumentException(s"Invalid type: $valueType")
    }
    // wrap in ArrayList because whatever asJava returns does not support shuffling
    toArrayList(valueCount, toObj)
  }

  private def toArrayList[T](valueCount: Int, intToObj: (Int) => T): util.ArrayList[T] = {
    new util.ArrayList(List.range(0, valueCount).map(intToObj).asJava)
  }


  def mapValuesOfList(key: String, value: java.util.List[_]): MapValue = {
    val paramsMap = mutable.Map[String, AnyRef](key -> value).asJava
    ValueUtils.asMapValue(paramsMap)
  }
}
