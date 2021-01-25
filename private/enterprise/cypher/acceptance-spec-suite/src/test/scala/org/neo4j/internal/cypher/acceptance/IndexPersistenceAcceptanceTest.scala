/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import java.nio.file.Path
import java.time.ZoneOffset

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.FileUtils
import org.neo4j.kernel.impl.index.schema.config.CrsConfig
import org.neo4j.test.rule.TestDirectory
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import scala.collection.Map

class IndexPersistenceAcceptanceTest extends IndexingTestSupport {

  private val testDir = TestDirectory.testDirectory(getClass)
  private var dbDir: Path = _

  override val cypherComparisonSupport = false

  override protected def initTest() {
    dbDir = testDir.prepareDirectoryForTest(System.currentTimeMillis().toString)
    startGraphDatabase(dbDir)
  }

  override protected def startGraphDatabase(storeDir: Path): Unit = {
    startGraphDatabaseWithConfig(storeDir, databaseConfig())
  }

  private def startGraphDatabaseWithConfig(storeDir: Path, config: Map[Setting[_], Object]): Unit = {
    val builder = graphDatabaseFactory(storeDir)
    config.foreach {
      case (setting, settingValue) => builder.setConfig(setting.asInstanceOf[Setting[Object]], settingValue)
    }
    managementService = builder.build()
    graphOps = managementService.database(DEFAULT_DATABASE_NAME)
    graph = new GraphDatabaseCypherService(graphOps)
  }

  private def restartGraphDatabase(config: Map[Setting[_], Object] = databaseConfig()): Unit = {
    managementService.shutdown()
    startGraphDatabaseWithConfig(dbDir, config)
  }

  override protected def stopTest() {
    try {
      super.stopTest()
    }
    finally {
      if (graph != null) managementService.shutdown()
      FileUtils.deleteDirectory(dbDir)
    }
  }

  private val wgs1 = Values.pointValue(CoordinateReferenceSystem.WGS84, 12.78, 56.7)
  private val wgs2 = Values.pointValue(CoordinateReferenceSystem.WGS84, 44.4, 44.5)
  private val wgs1_3d = Values.pointValue(CoordinateReferenceSystem.WGS84_3D, 12.78, 56.7, 100.0)
  private val car = Values.pointValue(CoordinateReferenceSystem.Cartesian, 1.0, 2.78)
  private val car_3d = Values.pointValue(CoordinateReferenceSystem.Cartesian_3D, 1.0, 2.78, 5.0)
  private val date = DateValue.epochDate( 1000 )
  private val dateTime = DateTimeValue.datetime( 1000, 100, ZoneOffset.UTC )
  private val localDateTime = LocalDateTimeValue.localDateTime( 1000, 100 )
  private val time = TimeValue.time( 1000, ZoneOffset.UTC )
  private val localTime = LocalTimeValue.localTime( 1000 )
  private val duration = DurationValue.duration( 1, 2, 3, 4 )

  private val values: Array[Value] = Array(wgs1, wgs2, wgs1_3d, car, car_3d, date, dateTime, localDateTime, time, localTime, duration)

  test("persisted indexed property should be seekable from node property") {
    createIndex()
    val node = createIndexedNode(wgs1)

    assertSeekMatchFor(wgs1, node)

    restartGraphDatabase()

    assertSeekMatchFor(wgs1, node)
  }

  test("different types of indexed property should survive restart") {
    createIndex()

    val nodes = values.map(createIndexedNode)

    assertScanMatch(nodes:_*)

    restartGraphDatabase()

    assertScanMatch(nodes:_*)
  }

  test("overwriting indexed property should work") {
    createIndex()
    val node = createIndexedNode(wgs1)
    assertSeekMatchFor(wgs1, node)

    setIndexedValue(node, wgs2)
    assertSeekMatchFor(wgs2, node)

    restartGraphDatabase()

    assertSeekMatchFor(wgs2, node)
  }

  test("create index before and after adding node and also survive restart") {
    val n1 = createIndexedNode(wgs1)
    createIndex()
    val n2 = createIndexedNode(wgs2)

    assertSeekMatchFor(wgs1, n1)
    assertSeekMatchFor(wgs2, n2)

    restartGraphDatabase()

    assertSeekMatchFor(wgs1, n1)
    assertSeekMatchFor(wgs2, n2)
  }

  test("create drop create index") {
    val n1 = createIndexedNode(wgs1)
    createIndex()

    assertSeekMatchFor(wgs1, n1)

    dropIndex()
    createIndex()

    assertSeekMatchFor(wgs1, n1)
  }

  test("change value of indexed node") {
    val n1 = createIndexedNode(wgs1)
    createIndex()

    assertSeekMatchFor(wgs1, n1)

    for ( value <- values ) {
      setIndexedValue(n1, value)
      assertSeekMatchFor(value, n1)
    }
  }

  test("Should not get new index configuration on database settings changes of WGS84 minimum x extent") {
    // remove the entire western hemisphere
    val wgs84_x_min = CrsConfig.group(CoordinateReferenceSystem.WGS84).min
    testIndexRestartWithSettingsChanges(Map(wgs84_x_min -> java.util.List.of(0.0, 0.0)))
  }

  private def testIndexRestartWithSettingsChanges(settings: Map[Setting[_], Object]): Unit = {
    createIndex()
    val data = (-180 to 180 by 10).flatMap { lon =>
      (-90 to 90 by 10).map { lat =>
        val point = Values.pointValue(CoordinateReferenceSystem.WGS84, lon, lat)
        point -> createIndexedNode(point)
      }
    }.toMap
    val expected = Values.pointValue(CoordinateReferenceSystem.WGS84, 10, 50)
    val node = data(expected)
    val min = Values.pointValue(CoordinateReferenceSystem.WGS84, 1, 41)
    val max = Values.pointValue(CoordinateReferenceSystem.WGS84, 19, 59)

    assertRangeScanFor(">", min, "<", max, node)

    restartGraphDatabase(settings)

    assertRangeScanFor(">", min, "<", max, node)

    dropIndex()
    createIndex()

    assertRangeScanFor(">", min, "<", max, node)
  }
}
