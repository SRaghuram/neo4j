/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planning.Fragment.Apply
import com.neo4j.fabric.planning.Fragment.Chain
import com.neo4j.fabric.planning.Fragment.Init
import com.neo4j.fabric.planning.Fragment.Leaf
import com.neo4j.fabric.planning.Fragment.Union
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.UseGraph

trait FragmentTestUtils {

  def init(use: Use, argumentColumns: Seq[String] = Seq(), importColumns: Seq[String] = Seq()): Init =
    Init(use, argumentColumns, importColumns)

  def unionAll(lhs: Fragment, rhs: Chain): Union = Union(false, lhs, rhs)
  def union(lhs: Fragment, rhs: Chain): Union = Union(true, lhs, rhs)

  implicit class FragBuilder(input: Chain) {
    def apply(fragmentInheritUse: Use => Fragment): Apply = Apply(input, fragmentInheritUse(input.use))
    def leaf(clauses: Seq[ast.Clause], outputColumns: Seq[String]): Leaf = Leaf(input, clauses, outputColumns)
  }

  private object AstUtils extends AstConstructionTestSupport

  val defaultGraphName: String = "default"
  val defaultGraph: UseGraph = AstUtils.use(AstUtils.varFor(defaultGraphName))
  val defaultUse: Use.Inherited = Use.Inherited(Use.Declared(defaultGraph))

  def pipeline(query: String): Pipeline.Instance

  def fragment(query: String): Fragment = {
    val state = pipeline(query).parseAndPrepare.process(query)
    val fragmenter = new FabricFragmenter(defaultGraphName, query, state.statement(), state.semantics())
    fragmenter.fragment
  }
}
