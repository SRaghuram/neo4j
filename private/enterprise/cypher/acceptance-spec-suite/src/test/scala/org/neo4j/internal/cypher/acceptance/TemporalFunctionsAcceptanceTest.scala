/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZonedDateTime

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.FakeClock
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.values.storable.DurationValue

class TemporalFunctionsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport with FakeClock {

  test("should get current default datetime") {
    val result = executeWith(Configs.UDF, "RETURN datetime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  test("should get current 'realtime' datetime") {
    val result = executeWith(Configs.UDF, "RETURN datetime.realtime() as now")

    val now = single(result.columnAs[ZonedDateTime]("now"))

    now shouldBe a[ZonedDateTime]
  }

  test("should get current default localdatetime") {
    val result = executeWith(Configs.UDF, "RETURN localdatetime() as now")

    val now = single(result.columnAs[LocalDateTime]("now"))

    now shouldBe a[LocalDateTime]
  }

  test("should get current default date") {
    val result = executeWith(Configs.UDF, "RETURN date() as now")

    val now = single(result.columnAs[LocalDate]("now"))

    now shouldBe a[LocalDate]
  }

  test("should get current default time") {
    val result = executeWith(Configs.UDF, "RETURN time() as now")

    val now = single(result.columnAs[OffsetTime]("now"))

    now shouldBe a[OffsetTime]
  }

  test("should get current default localtime") {
    val result = executeWith(Configs.UDF, "RETURN localtime() as now")

    val now = single(result.columnAs[LocalTime]("now"))

    now shouldBe a[LocalTime]
  }

  test("timestamp should be query local") {
    //older versions don't use the clock which we fake in this test
    val result = executeSingle("UNWIND range(1, 1000) AS ignore RETURN timestamp() AS t").toList

    result.map(m => m("t")).distinct should have size 1
  }

  test("timestamp should not be case sensitive") {
    val past = ZonedDateTime.of(1980, 3, 11, 0, 0, 0, 0,
      ZoneId.systemDefault())
    //older versions don't use the clock which we fake in this test
    val result = executeSingle("RETURN timeStamP() AS t").toList

    result.map(m => m("t")).head.asInstanceOf[Long] > past.toEpochSecond shouldBe true
  }

  test("should get right precision on duration") {
    val result = executeWith(Configs.UDF, "RETURN duration('P0.9Y') AS duration")
    val duration = single(result.columnAs[DurationValue]("duration"))

    duration should equal(DurationValue.parse("P10M24DT30196.8S"))
  }

  def single[T](values: Iterator[T]):T = {
    val value = values.next()
    values.hasNext shouldBe false
    value
  }
}
