/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.pipeline.Pipeline
import com.neo4j.fabric.planning.Fragment.Apply
import com.neo4j.fabric.planning.Fragment.Init
import com.neo4j.fabric.planning.Fragment.Leaf
import com.neo4j.fabric.planning.Fragment.Union
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport

trait FragmentTestUtils {

  def init(graph: ast.GraphSelection, argumentColumns: Seq[String] = Seq(), importColumns: Seq[String] = Seq()): Init = Init(graph, argumentColumns, importColumns)

  implicit class FragBuilder(input: Fragment) {
    def apply(fragment: Fragment): Apply = Apply(input, fragment)
    def unionAll(lhs: Fragment, rhs: Fragment): Union = Union(false, lhs, rhs, input)
    def union(lhs: Fragment, rhs: Fragment): Union = Union(true, lhs, rhs, input)
    def leaf(clauses: Seq[ast.Clause], outputColumns: Seq[String]): Leaf = Leaf(input, clauses, outputColumns)
  }

  private object AstUtils extends AstConstructionTestSupport

  val defaultGraphName: String = "default"
  val defaultGraph = AstUtils.use(AstUtils.varFor(defaultGraphName))

  def pipeline(query: String): Pipeline.Instance

  def fragment(query: String): Fragment = {
    val state = pipeline(query).parseAndPrepare.process(query)
    val fragmenter = new FabricFragmenter(defaultGraphName, query, state.statement(), state.semantics())
    fragmenter.fragment
  }
}
