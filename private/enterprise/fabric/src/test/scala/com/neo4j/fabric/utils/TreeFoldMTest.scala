/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.utils

import com.neo4j.fabric.utils.Monoid._
import com.neo4j.fabric.utils.TreeFoldM._
import com.neo4j.fabric.{AstHelp, Test}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.{expressions => exp}

class TreeFoldMTest extends Test with AstHelp {


  val tree: Expression =
    exp.ListLiteral(Seq(
      exp.FunctionInvocation(exp.Namespace()(?), exp.FunctionName("foo")(?), false, IndexedSeq(
        exp.FunctionInvocation(exp.Namespace()(?), exp.FunctionName("bar")(?), false, IndexedSeq(
          exp.Variable("grok")(?)
        ))(?),
        exp.FunctionInvocation(exp.Namespace()(?), exp.FunctionName("baz")(?), false, IndexedSeq(
          exp.Variable("grok")(?)
        ))(?)
      ))(?),
      exp.FunctionInvocation(exp.Namespace()(?), exp.FunctionName("boo")(?), false, IndexedSeq(
        exp.Variable("grok")(?)
      ))(?)
    ))(?)

  "Monoidal treeFold" - {
    "Stop" in {

      tree.treeFoldM {
        case v: exp.FunctionInvocation => Stop(List(v.functionName.name))
      } shouldEqual List("foo", "boo")

    }
    "Descend" in {
      tree.treeFoldM {
        case v: exp.FunctionInvocation => Descend(List(v.functionName.name))
      } shouldEqual List("foo", "bar", "baz", "boo")
    }

    "DescendWith" in {
      tree.treeFoldM {
        case v: exp.FunctionInvocation => DescendWith(List(v.functionName.name), List("::"))
      } shouldEqual List("foo", "bar", "::", "baz", "::", "::", "boo", "::")

    }
  }
}
