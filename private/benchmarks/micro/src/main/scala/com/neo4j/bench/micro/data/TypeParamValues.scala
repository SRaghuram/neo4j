/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data

import java.time.{LocalDate, LocalTime, ZoneOffset}
import java.util

import com.neo4j.bench.micro.benchmarks.RNGState
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian
import org.neo4j.values.storable.Values.pointValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._
import com.neo4j.bench.micro.data.ConstantGenerator.constant;
import com.neo4j.bench.micro.data.DiscreteGenerator.{discrete,Bucket};
import scala.collection.mutable

object TypeParamValues {
  final val INT = "int"
  final val LNG = "long"
  final val FLT = "float"
  final val DBL = "double"
  final val STR_SML = "small_string"
  final val STR_BIG = "big_string"
  final val POINT = "point"
  final val DATE_TIME = "date_time"
  final val LOCAL_DATE_TIME = "local_date_time"
  final val TIME = "time"
  final val LOCAL_TIME = "local_time"
  final val DATE = "date"
  final val DURATION = "duration"
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
      case DATE_TIME => i => {
        val epochSecondUTC = i
        val nanos = 0
        val ids = TemporalGenerator.ZONE_IDS
        DateTimeValue.datetime(epochSecondUTC, nanos, ids(epochSecondUTC % ids.length)).asObjectCopy()
      }
      case LOCAL_DATE_TIME => i => {
        val epochSecond = i
        val nano = i % 1000000000
        LocalDateTimeValue.localDateTime(epochSecond, nano).asObjectCopy()
      }
      case TIME => i => {
        val nanosOfDayUTC = i
        val hourRange = 2 * 18 + 1
        // time-zone offset in hours, from -18 to +18
        val hours = i % hourRange - 18
        TimeValue.time(nanosOfDayUTC, ZoneOffset.ofHours(hours)).asObjectCopy()
      }
      case LOCAL_TIME => i => LocalTime.of(i % 24, i % 60, i % 60, i % 1000000000)
      case DATE => i =>
        val year = i
        val month = i % 12 + 1
        val dayOfMonth = i % 28 + 1
        LocalDate.of(year, month, dayOfMonth)
      case DURATION => i =>
        val months = i
        val days = i
        val seconds = i
        val nanos = i
        DurationValue.duration(months, days, seconds, nanos)
      case POINT => i => pointValue(Cartesian, i, i);
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
