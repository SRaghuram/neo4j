/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import java.time.Duration
import java.util

import com.neo4j.fabric.FabricTest
import com.neo4j.fabric.ProcedureRegistryTestSupport
import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.Database
import com.neo4j.fabric.config.FabricConfig.GlobalDriverConfig
import com.neo4j.fabric.config.FabricConfig.Graph
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.planning.FabricQuery.Apply
import com.neo4j.fabric.planning.FabricQuery.ChainedQuery
import com.neo4j.fabric.planning.FabricQuery.Direct
import com.neo4j.fabric.planning.FabricQuery.LocalQuery
import com.neo4j.fabric.planning.FabricQuery.RemoteQuery
import com.neo4j.fabric.planning.FabricQuery.UnionQuery
import com.neo4j.fabric.planning.Fragment.ChainedFragment
import org.neo4j.configuration.Config
import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.monitoring.Monitors
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

import scala.reflect.ClassTag

//noinspection ZeroIndexToHead
class FabricPlannerTest extends FabricTest with AstConstructionTestSupport with ProcedureRegistryTestSupport with FragmentTestUtils {

  private val shardFoo0 = new Graph(0, FabricConfig.RemoteUri.create("bolt://foo:1234"), "s0", new NormalizedGraphName("shard-name-0"), null)
  private val shardFoo1 = new Graph(1, FabricConfig.RemoteUri.create("bolt://foo:1234"), "s1", new NormalizedGraphName("shard-name-1"), null)
  private val shardBar0 = new Graph(2, FabricConfig.RemoteUri.create("bolt://bar:1234"), "neo4j", new NormalizedGraphName("shard-name-2"), null)
  private val config = new FabricConfig(
    true,
    new Database(new NormalizedDatabaseName("mega"), util.Set.of(shardFoo0, shardFoo1, shardBar0)),
    util.List.of(), Duration.ZERO, Duration.ZERO, new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 1, null), new FabricConfig.DataStream(300, 1000, 50, 10)
  )
  private val params = MapValue.EMPTY
  private val monitors = new Monitors
  private val cypherConfig = CypherConfiguration.fromConfig(Config.defaults())
  private val signatures = new SignatureResolver(() => procedures)
  private val planner = FabricPlanner(config, cypherConfig, monitors, signatures)

  def pipeline(query: String): Pipeline.Instance =
    Pipeline.Instance(monitors, query, signatures)

  def plan(query: String, params: MapValue) =
    planner.instance(query, params, defaultGraphName).plan


  "Read/Write" - {

    "read" in {
      plan("MATCH (x) RETURN *", params).queryType
        .shouldEqual(QueryType.Read)
    }
    "read + known read proc" in {
      plan("MATCH (x) CALL my.ns.read() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.Read)
    }
    "read + known write proc" in {
      plan("MATCH (x) CALL my.ns.write() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.Write)
    }
    "read + unknown proc" in {
      plan("MATCH (x) CALL my.ns.unknown() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.ReadPlusUnresolved)
    }
    "write" in {
      plan("CREATE (x)", params).queryType
        .shouldEqual(QueryType.Write)
    }
    "write + known read proc" in {
      plan("CREATE (x) WITH * CALL my.ns.read() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.Write)
    }
    "write + known write proc" in {
      plan("CREATE (x) WITH * CALL my.ns.write() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.Write)
    }
    "write + unknown proc" in {
      plan("CREATE (x) WITH * CALL my.ns.unknown() YIELD a RETURN *", params).queryType
        .shouldEqual(QueryType.Write)
    }
    "per part" in {
      val pln = plan(
        """UNWIND [] AS x
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.unknown() YIELD a
          |  RETURN a AS a1
          |}
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.read() YIELD a
          |  RETURN a AS a2
          |}
          |CALL {
          |  USE g
          |  MATCH (n)
          |  CALL my.ns.write() YIELD a
          |  RETURN a AS a3
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.unknown() YIELD a
          |  RETURN a AS a4
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.read() YIELD a
          |  RETURN a AS a5
          |}
          |CALL {
          |  USE g
          |  CREATE (n) WITH *
          |  CALL my.ns.write() YIELD a
          |  RETURN a AS a6
          |}
          |RETURN *
          |""".stripMargin, params)

      val leafs = Stream
        .iterate(Option(pln.query)) {
          case Some(f: ChainedFragment) => Some(f.input)
          case _                        => None
        }
        .takeWhile(_.isDefined)
        .collect { case Some(f) => f }
        .toList
        .reverse

      leafs
        .map(QueryType.local)
        .shouldEqual(Seq(
          QueryType.Read,
          QueryType.Read,
          QueryType.ReadPlusUnresolved,
          QueryType.Read,
          QueryType.Write,
          QueryType.Write,
          QueryType.Write,
          QueryType.Write,
          QueryType.Read,
        ))

      pln.queryType
        .shouldEqual(QueryType.Write)
    }

  }

  "Cache:" - {

    "two equal input strings" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, signatures)

      val q =
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |WITH 3 AS z, y AS y
          |CALL {
          |  WITH 0 AS a
          |  RETURN 4 AS w
          |}
          |RETURN w, y
          """.stripMargin

      newPlanner.instance(q, params, defaultGraphName).plan
      newPlanner.instance(q, params, defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(1)
      newPlanner.queryCache.getHits.shouldEqual(1)
    }

    "two equal input strings with different params" in {
      val newPlanner = FabricPlanner(config, cypherConfig, monitors, signatures)

      val q =
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |WITH 3 AS z, y AS y
          |CALL {
          |  WITH 0 AS a
          |  RETURN 4 AS w
          |}
          |RETURN w, y
          """.stripMargin

      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of("a"))), defaultGraphName).plan
      newPlanner.instance(q, VirtualValues.map(Array("a"), Array(Values.of(1))), defaultGraphName).plan

      newPlanner.queryCache.getMisses.shouldEqual(2)
    }

  }

  "Options:" - {

    "allow EXPLAIN" in {
      val q =
        """EXPLAIN
          |RETURN 1 AS x
          |""".stripMargin

      plan(q, params)
        .check(_.executionType.shouldEqual(FabricPlan.EXPLAIN))
        .check(_.query.shouldEqual(
          init(defaultGraph).leaf(Seq(return_(literal(1).as("x"))), Seq("x"))
        ))
    }

    "disallow PROFILE" in {
      val q =
        """PROFILE
          |RETURN 1 AS x
          |""".stripMargin

      the[InvalidSemanticsException].thrownBy(plan(q, params))
        .check(_.getMessage.should(include("Query option: 'PROFILE' not supported in Fabric database")))
    }

    "disallow options" in {
      def shouldFail(qry: String, error: String) =
        the[InvalidSemanticsException].thrownBy(plan(qry, params))
          .check(_.getMessage.should(include(error)))

      shouldFail("CYPHER 3.5 RETURN 1", "Query option 'version' not supported in Fabric database")
      shouldFail("CYPHER planner=cost RETURN 1", "Query option 'planner' not supported in Fabric database")
      shouldFail("CYPHER runtime=parallel RETURN 1", "Query option 'runtime' not supported in Fabric database")
      shouldFail("CYPHER updateStrategy=eager RETURN 1", "Query option 'updateStrategy' not supported in Fabric database")
      shouldFail("CYPHER expressionEngine=compiled RETURN 1", "Query option 'expressionEngine' not supported in Fabric database")
      shouldFail("CYPHER operatorEngine=interpreted RETURN 1", "Query option 'operatorEngine' not supported in Fabric database")
      shouldFail("CYPHER interpretedPipesFallback=all RETURN 1", "Query option 'interpretedPipesFallback' not supported in Fabric database")
    }
  }

  object ClauseOps {
    def pretty = Prettifier(ExpressionStringifier())
  }

  implicit class ClauseOps(actual: Seq[Clause]) {
    def shouldEqualAst(expected: Seq[Clause]): Assertion = {
      try {
        actual.shouldEqual(expected)
      }
      catch {
        case e: TestFailedException =>
          println("--- Expected: ")
          expected.pprint()
          println("--- Actual: ")
          actual.pprint()
          throw e
      }
    }

    def pprint(): Unit =
      println(ClauseOps.pretty.asString(query(actual: _*)))
  }

  implicit class Caster[A](a: A) {
    def as[T](implicit ct: ClassTag[T]): T = {
      assert(ct.runtimeClass.isInstance(a), s"expected: ${ct.runtimeClass.getName}, was: ${a.getClass.getName}")
      a.asInstanceOf[T]
    }

    def check(f: A => Any): A = {
      f(a)
      a
    }
  }
}
