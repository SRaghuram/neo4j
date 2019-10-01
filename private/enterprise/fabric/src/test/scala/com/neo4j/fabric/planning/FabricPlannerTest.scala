/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import java.net.URI
import java.time.Duration
import java.util

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.{Database, GlobalDriverConfig, Graph}
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.planning.FabricQuery._
import com.neo4j.fabric.util.Errors.InvalidQueryException
import com.neo4j.fabric.{FabricTest, ProcedureRegistryTestSupport}
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, Clause, Query, SingleQuery, UnresolvedCall}
import org.neo4j.cypher.internal.v4_0.util.symbols.CTAny
import org.neo4j.exceptions.SyntaxException
import org.neo4j.monitoring.Monitors
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValue, VirtualValues}
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException

import scala.reflect.ClassTag

//noinspection ZeroIndexToHead
class FabricPlannerTest extends FabricTest with AstConstructionTestSupport with ProcedureRegistryTestSupport {

  private val shardFoo0 = new Graph(0, URI.create("bolt://foo"), "s0", "shard-name-0", null)
  private val shardFoo1 = new Graph(1, URI.create("bolt://foo"), "s1", "shard-name-1", null)
  private val shardBar0 = new Graph(2, URI.create("bolt://bar"), "neo4j", "shard-name-2", null)
  val config = new FabricConfig(
    true,
    new Database("mega", util.Set.of(shardFoo0, shardFoo1, shardBar0)),
    util.List.of(), 0L, Duration.ZERO, new GlobalDriverConfig(Duration.ZERO, Duration.ZERO, 1, null), new FabricConfig.DataStream(300, 1000, 50)
  )
  private val params = MapValue.EMPTY
  private val monitors = new Monitors
  private val cypherConfig = CypherConfiguration.fromConfig(Config.defaults())
  private val signatures = new SignatureResolver(() => procedures)
  private val planner = FabricPlanner(config, cypherConfig, monitors, signatures)

  "FROM handling: " - {

    "propagate FROM down and in" in {
      val q =
        """FROM g
          |WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(0).asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
        .check(_.queries(1).asApply.asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
        .check(_.queries(2).asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
    }

    "not propagate FROM out" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(0).asDirect.asLocalSingleQuery)
        .check(_.queries(1).asApply.asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
        .check(_.queries(2).asDirect.asLocalSingleQuery)
    }

    "override FROM in subquery" in {
      val q =
        """FROM g
          |WITH 1 AS x
          |CALL {
          |  FROM h
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(0).asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
        .check(_.queries(1).asApply.asDirect.asShardQuery.from.shouldEqual(from(varFor("h"))))
        .check(_.queries(2).asDirect.asShardQuery.from.shouldEqual(from(varFor("g"))))
    }

    "disallow embedded FROM" in {
      val q =
        """WITH 1 AS x
          |FROM i
          |RETURN x
          |""".stripMargin

      the[InvalidQueryException].thrownBy(
        planner.init(q, params).fabricQuery
      )
        .check(_.errors.size.shouldEqual(1))
        .check(_.errors(0).msg.should(include("FROM can only appear at the beginning of a (sub-)query")))

    }

    "disallow FROM at start of fragment" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  RETURN 2 AS y
          |}
          |FROM i
          |RETURN x
          |""".stripMargin

      the[InvalidQueryException].thrownBy(
        planner.init(q, params).fabricQuery
      )
        .check(_.errors.size.shouldEqual(1))
        .check(_.errors(0).msg.should(include("FROM can only appear at the beginning of a (sub-)query")))
    }

    "allow FROM to reference outer variable" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      planner.init(q, params).fabricQuery
        .asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardQuery.from.shouldEqual(from(function("g", varFor("x")))))
    }

    "allow FROM to reference imported variable" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH x
          |  FROM g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      planner.init(q, params).fabricQuery
        .asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardQuery.from.shouldEqual(from(function("g", varFor("x")))))
    }

    "disallow FROM to reference missing variable" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g(z)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      the[SyntaxException].thrownBy(
        planner.init(q, params).fabricQuery
      )
        .check(_.getMessage.should(include("Variable `z` not defined")))
    }

    "disallow FROM to reference outer variable after WITH" in {
      val q =
        """WITH 1 AS x, 2 AS y
          |CALL {
          |  WITH x
          |  FROM g(y)
          |  RETURN 2 AS z
          |}
          |RETURN z
          |""".stripMargin

      the[SyntaxException].thrownBy(
        planner.init(q, params).fabricQuery
      )
        .check(_.getMessage.should(include("Variable `y` not defined")))
    }
  }


  "Data flow: " - {

    "local subqueries as apply" in {
      val q =
        """WITH 1 AS y, 2 AS x
          |CALL {
          |  RETURN 3 AS z
          |}
          |RETURN y
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          return_(literal(3).as("z"))
        )))
    }

    "return inserted in local intermediate fragments" in {
      val q =
        """WITH 1 AS y, 2 AS x
          |CALL {
          |  RETURN 3 AS z
          |}
          |RETURN y
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(0).asDirect.asLocalSingleQuery.clauses.last
          .shouldEqual(return_(varFor("x").as("x"), varFor("y").as("y")))
        )
    }

    "local update fragments" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CREATE (z:A)
          |}
          |RETURN x
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          with_(literal(2).as("y")),
          create(nodePat("z", "A"))
        )))
    }

    "input data stream inserted in correlated local subquery" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH x
          |  WITH 2 AS y, x AS z
          |  RETURN y, z
          |}
          |RETURN x, y, z
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          input(varFor("x")),
          with_(varFor("x").as("x")),
          with_(literal(2).as("y"), varFor("x").as("z")),
          return_(varFor("y").as("y"), varFor("z").as("z"))
        )))
    }

    "subquery output aggregates as input to next fragment" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CALL {
          |    RETURN 3 AS z
          |  }
          |  RETURN y, z
          |}
          |RETURN x, y, z
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.head.shouldEqual(
          input(varFor("x"), varFor("y"), varFor("z"))
        ))
    }

    "nested local fragments subqueries" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CALL {
          |    RETURN 3 AS z
          |  }
          |  RETURN y
          |}
          |RETURN x, y
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asChainedQuery
          .check(_.queries(1).asApply.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            return_(literal(3).as("z")))
          ))
          .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            input(varFor("y"), varFor("z")),
            return_(varFor("y").as("y")))
          ))
        )
    }

    "imports translated to parameters in remote correlated fragments" in {
      val q =
        """WITH 1 AS x, 2 AS y
          |CALL {
          |  FROM g
          |  WITH x, y
          |  RETURN x AS x1, y AS y1
          |}
          |RETURN x, y, x1, y1
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqual(Seq(
          with_(parameter("@@x", CTAny).as("x"), parameter("@@y", CTAny).as("y")),
          with_(varFor("x").aliased, varFor("y").aliased),
          return_(varFor("x").as("x1"), varFor("y").as("y1"))
        )))
    }

    "local columns translated into parameters in remote trailing fragments" in {
      val q =
        """FROM g
          |WITH 1 AS x, 2 AS y
          |CALL {
          |  WITH x
          |  RETURN x AS z
          |}
          |RETURN x, y, z
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          with_(parameter("@@x", CTAny).as("x")),
          with_(varFor("x").aliased),
          return_(varFor("x").as("z"))
        )))
        .check(_.queries(2).asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          with_(parameter("@@x", CTAny).as("x"), parameter("@@y", CTAny).as("y"), parameter("@@z", CTAny).as("z")),
          return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
        )))
    }

    "outer columns are not made into parameters in remote fragments" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g
          |  WITH 2 AS y
          |  CALL {
          |    RETURN 3 AS z
          |  }
          |  RETURN y, z
          |}
          |RETURN x, y, z
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asChainedQuery
          .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
            return_(literal(3).as("z"))
          )))
          .check(_.queries(2).asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
            with_(parameter("@@y", CTAny).as("y"), parameter("@@z", CTAny).as("z")),
            return_(varFor("y").aliased, varFor("z").aliased)
          )))
        )
    }

    "remote fragment calling procedure" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g
          |  CALL some.procedure() YIELD z, y
          |}
          |RETURN x, y, z
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          call(Seq("some"), "procedure", yields = Some(Seq(varFor("z"), varFor("y")))),
          return_(varFor("y").aliased, varFor("z").aliased)
        )))
        .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqualAst(Seq(
          input(varFor("x"), varFor("y"), varFor("z")),
          return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
        )))
    }

    "remote fragment with update" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g
          |  CREATE (x:X)
          |}
          |RETURN x
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          create(nodePat("x", "X"))
        )))
        .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqualAst(Seq(
          input(varFor("x")),
          return_(varFor("x").aliased)
        )))
    }

    "remote fragments with update" in {
      val q =
        """FROM g
          |WITH 1 AS x
          |CREATE (y)
          |WITH x, y
          |CALL {
          |  FROM h
          |  WITH y
          |  CREATE (z)
          |}
          |RETURN x, y
          |""".stripMargin

      val pl = planner.init(q, params)
      pl.fabricQuery.asChainedQuery
        .check(_.queries(0).asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          with_(literal(1).as("x")),
          create(nodePat("y")),
          with_(varFor("x").aliased, varFor("y").aliased),
          return_(varFor("x").aliased, varFor("y").aliased),
        )))
        .check(_.queries(1).asApply.asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          with_(parameter("@@y", CTAny).as("y")),
          with_(varFor("y").aliased),
          create(nodePat("z"))
        )))
        .check(_.queries(2).asDirect.asShardSingleQuery.clauses.shouldEqualAst(Seq(
          with_(parameter("@@x", CTAny).as("x"), parameter("@@y", CTAny).as("y")),
          return_(varFor("x").aliased, varFor("y").aliased)
        )))
    }
  }

  "Acceptance:" - {

    "a single known procedure local query" in {
      val q =
        """CALL myProcedure()
          |""".stripMargin

      planner.plan(q, params)
        .check(_.asDirect.asLocalSingleQuery.clauses
          .check(_.size.shouldEqual(2))
          .check(_ (0).should(matchPattern { case _: ResolvedCall => }))
          .check(_ (1).shouldEqual(return_(varFor("a").aliased, varFor("b").aliased)))
        )
    }

    "a unknown procedure local query" in {
      val q =
        """CALL unknownProcedure() YIELD x, y
          |""".stripMargin

      planner.plan(q, params)
        .check(_.asDirect.asLocalSingleQuery.clauses
          .check(_.size.shouldEqual(2))
          .check(_ (0).should(matchPattern { case _: UnresolvedCall => }))
          .check(_ (1).shouldEqual(return_(varFor("x").aliased, varFor("y").aliased)))
        )
    }

    "a plain local query" in {
      val q =
        """MATCH (y)
          |RETURN y
          """.stripMargin

      planner.plan(q, params)
        .check(_.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          match_(nodePat("y")), return_(varFor("y").aliased)
        )))

    }

    "a plain shard query" in {
      val q =
        """FROM mega.shard0
          |MATCH (y)
          |RETURN y
          """.stripMargin

      planner
        .plan(q, params).asDirect.asShardQuery
        .check(_.from.expression.shouldEqual(prop(varFor("mega"), "shard0")))
        .check(_.query.part
          .as[SingleQuery].clauses.shouldEqual(Seq(match_(nodePat("y")), return_(varFor("y").aliased)))
        )
    }

    "a simple composite query" in {
      val q =
        """CALL {
          |  FROM mega.shard0
          |  MATCH (y)
          |  RETURN y
          |}
          |RETURN y
          """.stripMargin

      planner
        .plan(q, params).asChainedQuery
        .check(_.queries(0).asApply.asDirect.asShardQuery
          .check(_.from.expression.shouldEqual(prop(varFor("mega"), "shard0")))
          .check(_.asShardSingleQuery.clauses.shouldEqual(
            Seq(match_(nodePat("y")), return_(varFor("y").aliased))
          ))
        )
        .check(_.queries(1).asDirect.asLocalSingleQuery.clauses.shouldEqual(
          Seq(input(varFor("y")), return_(varFor("y").aliased))
        ))
    }

    "a flat composite query" in {
      val q =
        """UNWIND [1, 2] AS x
          |CALL {
          |  FROM mega.shard(x)
          |  MATCH (y)
          |  RETURN y
          |}
          |CALL {
          |  FROM mega.shard(y)
          |  RETURN 1 AS z, 2 AS w
          |}
          |RETURN x, y, z, w
          """.stripMargin

      planner.plan(q, params).asChainedQuery
        .check(_.queries(0).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          unwind(listOf(literalInt(1), literalInt(2)), varFor("x")),
          return_(varFor("x").aliased))
        ))
        .check(_.queries(1).asApply.asDirect.asShardQuery
          .check(_.from.expression.shouldEqual(function(Seq("mega"), "shard", varFor("x"))))
          .check(_.asShardSingleQuery.clauses.shouldEqual(Seq(
            match_(nodePat("y")),
            return_(varFor("y").aliased)
          )))
        )
        .check(_.queries(2).asApply.asDirect.asShardQuery
          .check(_.from.expression.shouldEqual(function(Seq("mega"), "shard", varFor("y"))))
          .check(_.asShardSingleQuery.clauses.shouldEqual(Seq(
            return_(literalInt(1).as("z"), literal(2).as("w"))
          )))
        )
        .check(_.queries(3).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          input(varFor("x"), varFor("y"), varFor("z"), varFor("w")),
          return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased, varFor("w").aliased)
        )))
    }

    "a nested composite query" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CALL {
          |    RETURN 3 AS z
          |  }
          |  RETURN y, z
          |}
          |RETURN x, y, z
          """.stripMargin

      planner.plan(q, params).asChainedQuery
        .check(_.queries(0).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          with_(literal(1).as("x")),
          return_(varFor("x").aliased))
        ))
        .check(_.queries(1).asApply.asChainedQuery
          .check(_.queries(0).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            with_(literal(2).as("y")),
            return_(varFor("y").aliased))
          ))
          .check(_.queries(1).asApply.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            return_(literal(3).as("z")))
          ))
          .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            input(varFor("y"), varFor("z")),
            return_(varFor("y").aliased, varFor("z").aliased))
          ))
        )
        .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          input(varFor("x"), varFor("y"), varFor("z")),
          return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
        )))
    }

    "a write query" in {
      val q =
        """
          |FROM mega.shard0
          |CREATE (y:Foo)
          """.stripMargin

      planner
        .plan(q, params).asDirect.asShardQuery
        .check(_.from.expression.shouldEqual(prop(varFor("mega"), "shard0")))
        .check(_.asShardSingleQuery.clauses.shouldEqual(Seq(
          create(nodePat("y", "Foo"))
        )))
    }

    "an outer union query" in {
      val q =
        """RETURN 1 AS x
          |UNION
          |RETURN 2 AS x
          """.stripMargin

      planner
        .plan(q, params)
        .as[UnionQuery]
        .check(_.distinct.shouldEqual(true))
        .check(_.lhs.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          return_(literalInt(1).as("x"))
        )))
        .check(_.rhs.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          return_(literalInt(2).as("x"))
        )))
    }

    "an inner union query" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  FROM g
          |  RETURN 1 AS y
          |  UNION
          |  WITH x
          |  RETURN 2 AS y
          |}
          |RETURN x, y
          """.stripMargin

      planner
        .plan(q, params).asChainedQuery
        .check(_.queries(0).asDirect.asLocalSingleQuery)
        .check(_.queries(1).asApply.as[UnionQuery]
          .check(_.distinct.shouldEqual(true))
          .check(_.lhs.asDirect.asShardQuery
            .check(_.from.expression.shouldEqual(varFor("g")))
            .check(_.asShardSingleQuery.clauses.shouldEqual(Seq(
              return_(literalInt(1).as("y"))
            )))
          )
          .check(_.rhs.asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
            input(varFor("x")),
            with_(varFor("x").aliased),
            return_(literalInt(2).as("y"))
          )))
        )
        .check(_.queries(2).asDirect.asLocalSingleQuery.clauses.shouldEqual(Seq(
          input(varFor("x"), varFor("y")),
          return_(varFor("x").aliased, varFor("y").aliased)
        )))
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

      newPlanner.plan(q, params)
      newPlanner.plan(q, params)

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

      newPlanner.plan(q, VirtualValues.map(Array("a"), Array(Values.of("a"))))
      newPlanner.plan(q, VirtualValues.map(Array("a"), Array(Values.of(1))))

      newPlanner.queryCache.getMisses.shouldEqual(2)
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

    def asLocalSingleQuery: SingleQuery =
      a.as[LocalQuery].query.state.statement()
        .as[Query].part
        .as[SingleQuery]

    def asShardSingleQuery: SingleQuery =
      a.as[RemoteQuery].query.part
        .as[SingleQuery]

    def asShardQuery: RemoteQuery =
      a.as[RemoteQuery]

    def asChainedQuery: ChainedQuery =
      a.as[ChainedQuery]

    def asDirect: FabricQuery =
      a.as[Direct].query

    def asApply: FabricQuery =
      a.as[Apply].query
  }

}
