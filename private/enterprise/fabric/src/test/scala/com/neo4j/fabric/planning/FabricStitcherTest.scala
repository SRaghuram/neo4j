/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.FabricTest
import com.neo4j.fabric.ProcedureRegistryTestSupport
import com.neo4j.fabric.planning.Use.Declared
import com.neo4j.fabric.planning.Use.Inherited
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.scalatest.Inside

class FabricStitcherTest
  extends FabricTest
    with AstConstructionTestSupport
    with ProcedureRegistryTestSupport
    with Inside
    with FragmentTestUtils {

  private def importParams(names: String*) =
    with_(names.map(v => parameter(Columns.paramName(v), ct.any).as(v)): _*)


  "Single-graph:" - {

    def stitching(fragment: Fragment) =
      FabricStitcher("", allowMultiGraph = false, None).convert(fragment)

    "single fragment" in {
      stitching(
        init(defaultUse).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(query(return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "single fragment, with USE" in {
      stitching(
        init(defaultUse).leaf(Seq(use("foo"), return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse).exec(query(return_(literal(1).as("a"))), Seq("a"))
      )
    }


    "single fragments with imports" in {
      stitching(
        init(defaultUse, Seq("x", "y"), Seq("y")).leaf(Seq(return_(literal(1).as("a"))), Seq("a"))
      ).shouldEqual(
        init(defaultUse, Seq("x", "y"), Seq("y")).exec(
          query(importParams("y"), return_(literal(1).as("a"))), Seq("a"))
      )
    }

    "nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(return_(literal(2).as("b"))),
            return_(literal(3).as("c"))
          ), Seq("c"))
      )
    }

    "nested fragment with nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(with_(literal(2).as("b"))), Seq("b"))
            .apply(u => init(Inherited(u)(pos), Seq("b"))
              .leaf(Seq(return_(literal(3).as("c"))), Seq("c")))
            .leaf(Seq(return_(literal(4).as("d"))), Seq("d")))
          .leaf(Seq(return_(literal(5).as("e"))), Seq("e"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(
              with_(literal(2).as("b")),
              subQuery(return_(literal(3).as("c"))),
              return_(literal(4).as("d"))
            ),
            return_(literal(5).as("e"))
          ), Seq("e"))
      )
    }

    "nested fragment after nested fragment" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(u => init(Inherited(u)(pos), Seq("a"))
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .apply(u => init(Inherited(u)(pos), Seq("a", "b"))
            .leaf(Seq(return_(literal(3).as("c"))), Seq("c")))
          .leaf(Seq(return_(literal(4).as("d"))), Seq("d"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("a")),
            subQuery(return_(literal(2).as("b"))),
            subQuery(return_(literal(3).as("c"))),
            return_(literal(4).as("d"))
          ), Seq("d"))
      )
    }

    "nested fragment directly after USE" in {
      stitching(
        init(Declared(use("foo")))
          .leaf(Seq(use("foo")), Seq())
          .apply(u => init(Inherited(u)(pos), Seq())
            .leaf(Seq(return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(Declared(use("foo")))
          .exec(query(
            subQuery(return_(literal(2).as("b"))),
            return_(literal(3).as("c"))
          ), Seq("c"))
      )
    }

    "union fragment, different imports" in {
      stitching(
        init(defaultUse, Seq("x", "y", "z"))
          .union(
            init(defaultUse, Seq("x", "y", "z"), Seq("y"))
              .leaf(Seq(return_(literal(1).as("a"))), Seq("a")),
            init(defaultUse, Seq("x", "y", "z"), Seq("z"))
              .leaf(Seq(return_(literal(2).as("a"))), Seq("a")))
      ).shouldEqual(
        init(defaultUse, Seq("x", "y", "z"), Seq("y", "z"))
          .exec(query(union(
            singleQuery(importParams("y"), return_(literal(1).as("a"))),
            singleQuery(importParams("z"), return_(literal(2).as("a")))
          )), Seq("a"))
      )
    }

    "nested union" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z"))), Seq("x", "y", "z"))
          .apply(u => init(Inherited(u)(pos), Seq("x", "y", "z"))
            .union(
              init(defaultUse, Seq("x", "y", "z"), Seq("y"))
                .leaf(Seq(with_(varFor("y").as("y")), return_(varFor("y").as("a"))), Seq("a")),
              init(defaultUse, Seq("x", "y", "z"), Seq("z"))
                .leaf(Seq(with_(varFor("z").as("z")), return_(varFor("z").as("a"))), Seq("a"))))
          .leaf(Seq(return_(literal(4).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(
            with_(literal(1).as("x"), literal(2).as("y"), literal(3).as("z")),
            subQuery(union(
              singleQuery(with_(varFor("y").as("y")), return_(varFor("y").as("a"))),
              singleQuery(with_(varFor("z").as("z")), return_(varFor("z").as("a")))
            )),
            return_(literal(4).as("c"))
          ), Seq("c"))
      )
    }
  }

  "Multi-graph:" - {

    def stitching(fragment: Fragment) = FabricStitcher("", allowMultiGraph = true, Some(defaultGraphName)).convert(fragment)

    "nested fragment, different USE" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"))
            .leaf(Seq(use("foo"), return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"))
            .exec(query(return_(literal(2).as("b"))), Seq("b")))
          .exec(query(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }

    "nested fragment, different USE, with imports" in {
      stitching(
        init(defaultUse)
          .leaf(Seq(with_(literal(1).as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"), Seq("a"))
            .leaf(Seq(with_(varFor("a").as("a")), use("foo"), return_(literal(2).as("b"))), Seq("b")))
          .leaf(Seq(return_(literal(3).as("c"))), Seq("c"))
      ).shouldEqual(
        init(defaultUse)
          .exec(query(with_(literal(1).as("a")), return_(varFor("a").as("a"))), Seq("a"))
          .apply(_ => init(Declared(use("foo")), Seq("a"), Seq("a"))
            .exec(query(with_(parameter("@@a", ct.any).as("a")), with_(varFor("a").as("a")), return_(literal(2).as("b"))), Seq("b")))
          .exec(query(input(varFor("a"), varFor("b")), return_(literal(3).as("c"))), Seq("c"))
      )
    }
  }
}
