/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.FabricTest
import com.neo4j.fabric.ProcedureRegistryTestSupport
import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.planning.Fragment.Apply
import com.neo4j.fabric.planning.Fragment.Leaf
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.exceptions.SyntaxException
import org.neo4j.monitoring.Monitors
import org.scalatest.Inside

//noinspection ZeroIndexToHead
class FabricFragmenterTest extends FabricTest with AstConstructionTestSupport with ProcedureRegistryTestSupport with Inside with FragmentTestUtils {

  private val monitors = new Monitors
  private val signatures = new SignatureResolver(() => procedures)

  def pipeline(query: String): Pipeline.Instance =
    Pipeline.Instance(monitors, query, signatures)




  "USE handling: " - {

    "disallow USE inside fragment" in {
      the[SyntaxException].thrownBy(
        fragment(
          """WITH 1 AS x
            |USE i
            |RETURN x
            |""".stripMargin)
      ).getMessage.should(include("USE can only appear at the beginning of a (sub-)query"))

    }

    "not propagate USE out" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin)

      frag.graph.shouldEqual(defaultGraph)
      inside(frag) { case Leaf(Apply(_, inner: Leaf), _, _) =>
        inner.graph.shouldEqual(use("g"))
      }
    }

    "disallow USE at start of non-initial fragment" in {
      the[SyntaxException].thrownBy(
        fragment(
          """WITH 1 AS x
            |CALL {
            |  RETURN 2 AS y
            |}
            |USE i
            |RETURN x
            |""".stripMargin)
      ).getMessage.should(include("USE can only appear at the beginning of a (sub-)query"))
    }

    "allow USE to reference outer variable" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin)

      inside(frag) { case Leaf(Apply(_, inner: Leaf), _, _) =>
        inner.graph.shouldEqual(use(function("g", varFor("x"))))
      }
    }

    "allow USE to reference imported variable" in {
      val frag = fragment(
        """WITH 1 AS x
          |CALL {
          |  WITH x
          |  USE g(x)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin)

      inside(frag) { case Leaf(Apply(_, inner: Leaf), _, _) =>
        inner.graph.shouldEqual(use(function("g", varFor("x"))))
      }
    }

    "disallow USE to reference missing variable" in {
      val q =
        """WITH 1 AS x
          |CALL {
          |  USE g(z)
          |  RETURN 2 AS y
          |}
          |RETURN x
          |""".stripMargin

      the[SyntaxException].thrownBy(
        fragment(
          """WITH 1 AS x
            |CALL {
            |  USE g(z)
            |  RETURN 2 AS y
            |}
            |RETURN x
            |""".stripMargin)
      ).getMessage.should(include("Variable `z` not defined"))
    }

    "disallow USE to reference outer variable after WITH" in {

      the[SyntaxException].thrownBy(
        fragment(
          """WITH 1 AS x, 2 AS y
            |CALL {
            |  WITH x
            |  USE g(y)
            |  RETURN 2 AS z
            |}
            |RETURN z
            |""".stripMargin)
      ).getMessage.should(include("Variable `y` not defined"))
    }
  }

  "Full queries:" - {

    "plain query" in {
      fragment(
        """WITH 1 AS a
          |RETURN a
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "a"), returnVars("a")), Seq("a"))
      )
    }

    "plain query with USE" in {
      fragment(
        """USE bar
          |WITH 1 AS a
          |RETURN a
          |""".stripMargin
      ).shouldEqual(
        init(use("bar"))
          .leaf(Seq(use("bar"), withLit(1, "a"), returnVars("a")), Seq("a"))
      )
    }

    "subquery with USE" in {
      fragment(
        """WITH 1 AS a
          |CALL {
          |  USE g
          |  RETURN 2 AS b
          |}
          |RETURN a, b
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "a")), Seq("a"))
          .apply(init(use("g"), Seq("a"), Seq())
            .leaf(Seq(use("g"), returnLit(2 -> "b")), Seq("b"))
          )
          .leaf(Seq(returnVars("a", "b")), Seq("a", "b"))
      )
    }

    "correlated subquery" in {
      fragment(
        """WITH 1 AS a
          |CALL {
          |  WITH a
          |  RETURN a AS b
          |}
          |RETURN a, b
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "a")), Seq("a"))
          .apply(init(defaultGraph, Seq("a"), Seq("a"))
            .leaf(Seq(withVar("a"), returnAliased("a" -> "b")), Seq("b"))
          )
          .leaf(Seq(returnVars("a", "b")), Seq("a", "b"))
      )
    }

    "nested queries, with scoped USE and importing WITH" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  WITH 2 AS y
          |  CALL {
          |    USE foo
          |    RETURN 3 AS z
          |  }
          |  CALL {
          |    WITH y
          |    RETURN 4 AS w
          |  }
          |  RETURN w, y, z
          |}
          |RETURN x, w, y, z
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(init(defaultGraph, Seq("x"))
            .leaf(Seq(withLit(2, "y")), Seq("y"))
            .apply(init(use("foo"), Seq("y"))
              .leaf(Seq(use("foo"), returnLit(3 -> "z")), Seq("z"))
            )
            .apply(init(defaultGraph, Seq("y", "z"), Seq("y"))
              .leaf(Seq(withVar("y"), returnLit(4 -> "w")), Seq("w"))
            )
            .leaf(Seq(returnVars("w", "y", "z")), Seq("w", "y", "z"))
          )
          .leaf(Seq(returnVars("x", "w", "y", "z")), Seq("x", "w", "y", "z"))
      )
    }

    "union query, with different USE " in {
      fragment(
        """USE foo
          |RETURN 1 AS y
          |  UNION
          |USE bar
          |RETURN 2 AS y
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .union(
            init(use("foo")).leaf(Seq(use("foo"), returnLit(1 -> "y")), Seq("y")),
            init(use("bar")).leaf(Seq(use("bar"), returnLit(2 -> "y")), Seq("y")),
          )
      )
    }

    "nested union query, with USE and importing WITH" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  USE foo
          |  RETURN 1 AS y
          |    UNION
          |  WITH x
          |  RETURN 2 AS y
          |}
          |RETURN x, y
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(
            init(defaultGraph, Seq("x")).union(
              init(use("foo"), Seq("x"))
                .leaf(Seq(use("foo"), returnLit(1 -> "y")), Seq("y")),
              init(defaultGraph, Seq("x"), Seq("x"))
                .leaf(Seq(withVar("x"), returnLit(2 -> "y")), Seq("y")),
            )
          )
          .leaf(Seq(returnVars("x", "y")), Seq("x", "y"))
      )
    }

    "subquery calling procedure" in {
      fragment(
        """WITH 1 AS x
          |CALL {
          |  USE g
          |  CALL some.procedure() YIELD z, y
          |  RETURN z, y
          |}
          |RETURN x, y, z
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(withLit(1, "x")), Seq("x"))
          .apply(
            init(use("g"), Seq("x"))
              .leaf(Seq(
                use("g"),
                call(Seq("some"), "procedure", yields = Some(Seq(varFor("z"), varFor("y")))),
                returnVars("z", "y")
              ), Seq("z", "y"))
          )
          .leaf(Seq(returnVars("x", "y", "z")), Seq("x", "y", "z"))

      )
    }

  }


  "Procedures:" - {

    "a single known procedure local query" in {
      fragment(
        """CALL my.ns.myProcedure()
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(
            resolved(call(Seq("my", "ns"), "myProcedure", Some(Seq()), Some(Seq(varFor("a"), varFor("b"))))),
            returnVars("a", "b"),
          ), Seq("a", "b"))
      )
    }

    "a single known procedure local query without args" in {
      fragment(
        """CALL my.ns.myProcedure
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(
            resolved(call(Seq("my", "ns"), "myProcedure", Some(Seq()), Some(Seq(varFor("a"), varFor("b"))))),
            returnVars("a", "b"),
          ), Seq("a", "b"))
      )
    }

    "a single known procedure local query with args" in {
      fragment(
        """CALL my.ns.myProcedure2(1)
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(
            resolved(call(Seq("my", "ns"), "myProcedure2", Some(Seq(literal(1))), Some(Seq(varFor("a"), varFor("b"))))),
            returnVars("a", "b"),
          ), Seq("a", "b"))
      )
    }

    "a unknown procedure local query" in {
      fragment(
        """CALL unknownProcedure() YIELD x, y
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(call(Seq(), "unknownProcedure", Some(Seq()), Some(Seq(varFor("x"), varFor("y"))))), Seq("x", "y"))
      )
    }

    "a known function" in {
      fragment(
        """RETURN const0() AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(return_(resolved(function("const0")).as("x"))), Seq("x"))
      )
    }

    "a known function with namespace and args" in {
      fragment(
        """RETURN my.ns.const0(1) AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(return_(resolved(function(Seq("my", "ns"), "const0", literal(1))).as("x"))), Seq("x"))
      )
    }

    "an unknown function" in {
      fragment(
        """RETURN my.unknown() AS x
          |""".stripMargin
      ).shouldEqual(
        init(defaultGraph)
          .leaf(Seq(return_(function(Seq("my"), "unknown").as("x"))), Seq("x"))
      )
    }
  }

  private def use(name: String): UseGraph =
    use(varFor(name))

  private def withLit(num: Int, varName: String): With =
    with_(literal(num).as(varName))

  private def withVar(varName: String): With =
    with_(varFor(varName).as(varName))

  private def returnLit(items: (Int, String)*) =
    return_(items.map(i => literal(i._1).as(i._2)): _*)

  private def returnVars(vars: String*) =
    return_(vars.map(v => varFor(v).aliased): _*)

  private def returnAliased(vars: (String, String)*) =
    return_(vars.map(v => varFor(v._1).as(v._2)): _*)

  private def resolved(unresolved: UnresolvedCall): ResolvedCall =
    ResolvedCall(signatures.procedureSignature)(unresolved)

  private def resolved(unresolved: FunctionInvocation): ResolvedFunctionInvocation =
    ResolvedFunctionInvocation(signatures.functionSignature)(unresolved)
}
