/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.cypher.acceptance

import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasDropped
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasStarted
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseHasStopped
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers
import com.neo4j.causalclustering.common.Cluster
import com.neo4j.causalclustering.core.consensus.roles.Role
import com.neo4j.test.causalclustering.ClusterConfig
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.test.rule.TestDirectory
import org.neo4j.test.rule.TestDirectory.testDirectory
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.Random

class ClusterAdministrationCommandAcceptanceTest extends CypherFunSuite with Eventually {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(60, Seconds)), interval = scaled(Span(5, Seconds)))

  def randomString: String = Random.alphanumeric.take(8).mkString("")

  val testDir: TestDirectory = prepareTestDirectory
  val cluster: Cluster = ClusterConfig.createCluster(
    testDir.directory(randomString),
    ClusterConfig
      .clusterConfig()
      .withNumberOfCoreMembers(3)
      .withNumberOfReadReplicas(1)
      .withInstanceCoreParam(GraphDatabaseSettings.auth_enabled, _ => "true")
  )

  override def afterEach(): Unit = {
    super.afterEach()
    cluster.shutdown()
  }

  test("create database on cluster should wait for database to be ready") {
    // GIVEN
    cluster.start()

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("CREATE DATABASE db1 WAIT")
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 1
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    })

    // THEN
    assertDatabaseHasStarted("db1", cluster)
    CausalClusteringTestHelpers.dropDatabase("db1", cluster)
  }

  test("create database on cluster with security should wait for database to be ready") {
    // GIVEN
    cluster.start()
    cluster.systemTx((_,tx) => {
      tx.execute("CREATE USER $user SET PASSWORD $password CHANGE NOT REQUIRED", Map[String, Object]("user" -> "joe", "password" -> "soap").asJava)
      tx.execute("GRANT ROLE admin to joe")
      tx.commit()
    })

    // WHEN
    val work: BiConsumer[GraphDatabaseFacade, Transaction] = (_, tx) => {
      val r = tx.execute("CREATE DATABASE db1 WAIT")
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 1
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    }
    cluster.coreTx(SYSTEM_DATABASE_NAME, Role.LEADER, work, 3, TimeUnit.MINUTES, "joe", "soap")

    // THEN
    assertDatabaseHasStarted("db1", cluster)
    CausalClusteringTestHelpers.dropDatabase("db1", cluster)
  }

  test("create or replace database that already exists on cluster should wait for database to be ready") {
    // GIVEN
    cluster.start()
    CausalClusteringTestHelpers.createDatabase("db1", cluster, true)
    assertDatabaseHasStarted("db1", cluster)

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("CREATE OR REPLACE DATABASE db1 WAIT")
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 2
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    })

    // THEN
    assertDatabaseHasStarted("db1", cluster)
    CausalClusteringTestHelpers.dropDatabase("db1", cluster)
  }

  test("create database IF not EXISTS does not wait") {
    // GIVEN
    cluster.start()
    CausalClusteringTestHelpers.createDatabase("existing", cluster, true)
    assertDatabaseHasStarted("existing", cluster)

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("CREATE DATABASE $existing IF NOT EXISTS WAIT", Map[String, Object]("existing" -> "existing").asJava).asScala.toList
      // THEN
      r.foreach(row => {
        row.get("state") shouldBe "CaughtUp"
        row.get("success") shouldBe true
      })
      r.length shouldBe 1
    })

  }

  test("stop database on cluster should wait for database to stop") {
    // GIVEN
    cluster.start()

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("STOP DATABASE neo4j WAIT")
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 1
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    })

    // THEN
    assertDatabaseHasStopped("neo4j", cluster)
    CausalClusteringTestHelpers.startDatabase("neo4j", cluster)
  }

  test("start database on cluster should wait for database to start") {
    // GIVEN
    cluster.start()
    CausalClusteringTestHelpers.stopDatabase("neo4j", cluster)
    eventually {
      cluster.systemTx((_, tx) => {
        val r = tx.execute("SHOW DATABASE neo4j").asScala.toList
        r.length shouldBe 4
        r.foreach(row => {
          row.get("requestedStatus") shouldBe "offline"
          row.get("currentStatus") shouldBe "offline"
        })
      })
    }

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("START DATABASE neo4j WAIT")
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 1
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    })

    // THEN
    assertDatabaseHasStarted("neo4j", cluster)
  }

  test("drop non-existent database IF EXISTS does not wait") {
    // GIVEN
    cluster.start()

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("DROP DATABASE $notthere IF EXISTS WAIT", Map[String, Object]("notthere" -> "notthere").asJava).asScala.toList
      // THEN
      r.foreach(row => {
        row.get("state") shouldBe "CaughtUp"
        row.get("success") shouldBe true
      })
      r.length shouldBe 1
    })

  }

  test("drop database on cluster should wait for database to be dropped") {
    // GIVEN
    cluster.start()
    CausalClusteringTestHelpers.createDatabase("foo", cluster)
    eventually {
      cluster.systemTx((_, tx) => {
        val r = tx.execute("SHOW DATABASE foo").asScala.toList
        r.length shouldBe 4
        r.foreach(row => {
          row.get("requestedStatus") shouldBe "online"
          row.get("currentStatus") shouldBe "online"
        })
      })
    }

    // WHEN
    cluster.systemTx((_, tx) => {
      val r = tx.execute("DROP DATABASE $bar WAIT", Map[String, Object]("bar" -> "foo").asJava)
      // THEN
      r.getQueryStatistics.getSystemUpdates shouldBe 1
      r.columnAs[String]("state").forEachRemaining(state => state shouldBe "CaughtUp")
    })

    // THEN
    assertDatabaseHasDropped("foo", cluster)
  }

  private def prepareTestDirectory: TestDirectory = {
    val t = testDirectory()
    t.prepareDirectory(getClass, randomString)
    t
  }

}
