/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data

import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.util
import java.util.Collections

import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values.pointValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

  def shuffledListOf[T](valueType: String, valueCount: Int, distinctCount: Int): util.List[T] = {
    assert(distinctCount <= valueCount)
    val baseList = ascendingListOf(valueType, distinctCount)
    val list = new ArrayBuffer[T]()
    var listLength = list.size
    while (listLength < valueCount) {
      val difference = valueCount - listLength
      if (difference >= distinctCount) {
        list ++= baseList
      } else {
        val toAdd = baseList.take(difference)
        list ++= toAdd
      }
      listLength = list.size
    }
    val javaList = toArrayList(list)
    Collections.shuffle(javaList)
    javaList
  }

  def listOf(valueType: String, valueCount: Int): util.List[_] = {
    // wrap in ArrayList because whatever asJava returns does not support shuffling
    toArrayList(ascendingListOf(valueType, valueCount))
  }

  private def ascendingListOf[T](valueType: String, valueCount: Int): List[T] = {
    def intToT(i: Int): T = {
      val intToObj: Int => Any = valueType match {
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
      intToObj(i).asInstanceOf[T]
    }
    List.range(0, valueCount).map(intToT)
  }

  private def toArrayList[T](seq: Seq[T]): util.ArrayList[T] = {
    new util.ArrayList(seq.asJava)
  }

  def mapValuesOfList(key: String, value: java.util.List[_]): MapValue = {
    val paramsMap = mutable.Map[String, AnyRef](key -> value).asJava
    ValueUtils.asMapValue(paramsMap)
  }
}
