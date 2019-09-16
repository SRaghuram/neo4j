/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner

import java.net.URI
import java.time.Duration
import java.util

import com.neo4j.fabric.config.FabricConfig
import com.neo4j.fabric.config.FabricConfig.{Database, Graph, RemoteGraphDriver}
import com.neo4j.fabric.planner.Errors.InvalidQueryException
import com.neo4j.fabric.planner.FabricQuery.{ChainedQuery, LocalQuery, ShardQuery, UnionQuery}
import com.neo4j.fabric.{AstHelp, Test}
import org.neo4j.cypher.internal.v4_0.ast.{AstConstructionTestSupport, Query, SingleQuery}
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, NodePattern, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.monitoring.Monitors
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{MapValue, VirtualValues}

import scala.reflect.ClassTag

class FabricPlannerTest extends Test with AstHelp with AstConstructionTestSupport {

  private val shardFoo0 = new Graph(0, URI.create("bolt://foo"), "s0", "shard-name-0")
  private val shardFoo1 = new Graph(1, URI.create("bolt://foo"), "s1", "shard-name-1")
  private val shardBar0 = new Graph(2, URI.create("bolt://bar"), "neo4j", "shard-name-2")
  val config = new FabricConfig(
    true,
    new Database("mega", util.Set.of(shardFoo0, shardFoo1, shardBar0)),
    util.List.of(), 0L, Duration.ZERO, new RemoteGraphDriver(Duration.ZERO, Duration.ZERO), new FabricConfig.DataStream(300, 1000, 50)
  )
  private val params = MapValue.EMPTY

  private val monitors = new Monitors();
  private val planner = FabricPlanner(config, monitors)

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
      a.as[ShardQuery].query
        .as[Query].part
        .as[SingleQuery]
  }

  "Planner should" - {

    "accept" - {

      "a single procedure call" in {
        val q =
          """
            |CALL dbms.cluster.routing.getRoutingTable()
          """.stripMargin

        planner
          .plan(q, params)
          .asLocalSingleQuery.clauses.shouldEqual(Seq(call(Seq("dbms", "cluster", "routing"), "getRoutingTable")))
      }

      "a plain local query" in {
        val q =
          """MATCH (y)
            |RETURN y
          """.stripMargin

        planner
          .plan(q, params)
          .asLocalSingleQuery.clauses.shouldEqual(Seq(match_(node("y")), return_(varFor("y").aliased)))
      }

      "a plain shard query" in {
        val q =
          """FROM mega.shard0
            |MATCH (y)
            |RETURN y
          """.stripMargin

        planner
          .plan(q, params)
          .as[ShardQuery]
          .check(_.from.expression.shouldEqual(prop(varFor("mega"), "shard0")))
          .check(_.query.part
            .as[SingleQuery].clauses.shouldEqual(Seq(match_(node("y")), return_(varFor("y").aliased)))
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
          .plan(q, params)
          .as[ChainedQuery]
          .check(_.lhs
            .as[ShardQuery]
            .check(_.from.expression.shouldEqual(prop(varFor("mega"), "shard0")))
            .check(_.query.part.as[SingleQuery].clauses.shouldEqual(Seq(match_(node("y")), return_(varFor("y").aliased)))
            )
          )
          .check(_.rhs
            .asLocalSingleQuery.clauses.shouldEqual(Seq(input(varFor("y")), return_(varFor("y").aliased)))
          )
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

        planner.plan(q, params)
          .as[ChainedQuery]
          .check(_.lhs.as[ChainedQuery]
            .check(_.lhs.as[ChainedQuery]
              .check(_.lhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
                unwind(listOf(literalInt(1), literalInt(2)), varFor("x")),
                return_(varFor("x").aliased))
              ))
              .check(_.rhs.as[ShardQuery]
                .check(_.from.expression.shouldEqual(function(Seq("mega"), "shard", varFor("x"))))
                .check(_.query.part.as[SingleQuery].clauses.shouldEqual(Seq(
                  match_(node("y")),
                  return_(varFor("y").aliased)
                )))
              )
            )
            .check(_.rhs.as[ShardQuery]
              .check(_.from.expression.shouldEqual(function(Seq("mega"), "shard", varFor("y"))))
              .check(_.query.part.as[SingleQuery].clauses.shouldEqual(Seq(
                return_(literalInt(1).as("z"), literal(2).as("w"))
              )))
            )
          )
          .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
            input(varFor("x"), varFor("y"), varFor("z"), varFor("w")),
            return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased, varFor("w").aliased)
          )))
      }

      "a flat composite query with horizon" in {
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

        planner
          .plan(q, params).as[ChainedQuery]
          .check(_.lhs.as[ChainedQuery]
            .check(_.lhs.as[ChainedQuery]
              .check(_.lhs.as[ChainedQuery]
                .check(_.lhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
                  with_(literalInt(1).as("x")),
                  return_(varFor("x").aliased))))
                .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
                  input(varFor("x")),
                  return_(varFor("x").aliased, literalInt(2).as("y")))))
              )
              .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
                input(varFor("x"), varFor("y")),
                with_(literalInt(3).as("z"), varFor("y").aliased),
                return_(varFor("y").aliased, varFor("z").aliased))))
            )
            .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
              input(varFor("y"), varFor("z")),
              with_(varFor("y").aliased, varFor("z").aliased, literalInt(0).as("a")),
              return_(varFor("y").aliased, varFor("z").aliased, literalInt(4).as("w")))))
          )
          .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(
            input(varFor("y"), varFor("z"), varFor("w")),
            return_(varFor("w").aliased, varFor("y").aliased))))
      }

      "a nested composite query" ignore {
        // TODO: Fix data flow analysis for nested subqueries
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

        planner
          .plan(q, params).as[ChainedQuery]
          .check(_.lhs.as[ChainedQuery]
            .check(_.lhs.asLocalSingleQuery.clauses.shouldEqual(Seq(with_(literalInt(1).as("x")), return_(varFor("x").aliased))))
            .check(_.rhs.as[ChainedQuery]
              .check(_.lhs.as[ChainedQuery]
                .check(_.lhs.asLocalSingleQuery.clauses.shouldEqual(Seq(with_(literalInt(2).as("y")), return_(varFor("y").aliased))))
                .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(input(varFor("y")), return_(literalInt(3).as("z")))))
              )
              .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(input(varFor("y"), varFor("z")), return_(varFor("y").aliased, varFor("z").aliased))))
            )
          )
          .check(_.rhs.asLocalSingleQuery.clauses.shouldEqual(Seq(input(varFor("x"), varFor("y"), varFor("z")), return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased))))
      }

      "a write query" in {
        val q =
          """
            |FROM mega.shard0
            |CREATE (y:Foo)
          """.stripMargin

        planner
          .plan(q, params)
          .as[ShardQuery]
          .check(
            _.from.expression.shouldEqual(prop(varFor("mega"), "shard0")
            ))
          .check(_.query.part
            .as[SingleQuery].clauses.shouldEqual(Seq(create(node("y", "Foo")(pos))))
          )
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
          .check(_.lhs
            .asLocalSingleQuery.clauses.shouldEqual(Seq(return_(literalInt(1).as("x"))))
          )
          .check(_.rhs
            .asLocalSingleQuery.clauses.shouldEqual(Seq(return_(literalInt(2).as("x"))))
          )
      }

    }

    "output" - {
      "aligned returns and inputs (1)" in {

        val q =
          """CALL {
            |   FROM mega.graph(0)
            |   RETURN 1 AS cid
            |}
            |UNWIND [2] AS gid
            |CALL {
            |   FROM mega.graph(gid)
            |   RETURN 3 AS z
            |}
            |RETURN *
            |""".stripMargin

        planner.plan(q, params)
          .check(_.as[ChainedQuery]
            .check(_.lhs.as[ChainedQuery]
              .check(_.lhs.as[ChainedQuery]
                .rhs.asLocalSingleQuery.clauses.last.shouldEqual(return_(varFor("gid").aliased, varFor("cid").aliased)))
              .check(_.rhs.as[ShardQuery].input.shouldEqual(Seq("gid", "cid")))
            )
          )
      }

      "aligned returns and inputs (2)" in {

        val q =
          """CALL {
            |   FROM mega.graph(0)
            |   RETURN 1 AS x
            |}
            |UNWIND [2] AS y
            |CALL {
            |   FROM mega.graph(y)
            |   RETURN 3 AS z
            |}
            |RETURN *
            |""".stripMargin

        planner.plan(q, params)
          .check(_.as[ChainedQuery]
            .check(_.lhs.as[ChainedQuery]
              .check(_.lhs.as[ChainedQuery]
                .rhs.asLocalSingleQuery.clauses.last.shouldEqual(return_(varFor("x").aliased, varFor("y").aliased))
              )
              .check(_.rhs.as[ShardQuery].input.shouldEqual(Seq("x", "y"))
              )
            )
          )
      }
    }

    "reject" - {
      "embedded FROM" in {
        val q =
          """WITH 1 AS x
            |FROM foo.bar
            |RETURN x
          """.stripMargin

        a[InvalidQueryException] shouldBe thrownBy {
          planner.plan(q, params)
        }
      }
    }

    "cache" - {

      "two equal input strings" in {
        val newPlanner = FabricPlanner(config, monitors)

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
        val newPlanner = FabricPlanner(config, monitors)

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

  }

  def node(name: String): NodePattern =
    NodePattern(Some(Variable(name)(pos)), Seq(), None)(pos)

  def node(name: String, labels: String*)(implicit p: InputPosition): NodePattern =
    NodePattern(Some(Variable(name)(p)), labels.map(LabelName(_)(p)), None)(p)

}
