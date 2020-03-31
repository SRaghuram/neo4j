/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planning

import com.neo4j.fabric.pipeline.FabricFrontEnd
import com.neo4j.fabric.pipeline.SignatureResolver
import com.neo4j.fabric.planning.Fragment.Apply
import com.neo4j.fabric.planning.Fragment.Chain
import com.neo4j.fabric.planning.Fragment.Init
import com.neo4j.fabric.planning.Fragment.Leaf
import com.neo4j.fabric.planning.Fragment.Exec
import com.neo4j.fabric.planning.Fragment.Union
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.monitoring.Monitors
import org.neo4j.procedure.impl.GlobalProceduresRegistry
import org.neo4j.values.virtual.MapValue

trait FragmentTestUtils {

  def init(use: Use, argumentColumns: Seq[String] = Seq(), importColumns: Seq[String] = Seq()): Init =
    Init(use, argumentColumns, importColumns)

  implicit class FragBuilder(input: Chain) {
    def apply(fragmentInheritUse: Use => Fragment): Apply = Apply(input, fragmentInheritUse(input.use))
    def leaf(clauses: Seq[ast.Clause], outputColumns: Seq[String]): Leaf = Leaf(input, clauses, outputColumns)
    def exec(query: Query, outputColumns: Seq[String]): Exec = Exec(input, query, outputColumns)
  }

  implicit class FragBuilderInit(input: Fragment.Init) {
    def union(lhs: Fragment, rhs: Chain): Union = Union(input, true, lhs, rhs)
    def unionAll(lhs: Fragment, rhs: Chain): Union = Union(input, false, lhs, rhs)
  }

  object ct {
    val any: AnyType = org.neo4j.cypher.internal.util.symbols.CTAny
  }

  private object AstUtils extends AstConstructionTestSupport

  def use(name: String): UseGraph = AstUtils.use(AstUtils.varFor(name))

  val defaultGraphName: String = "default"
  val defaultGraph: UseGraph = use(defaultGraphName)
  val defaultUse: Use.Inherited = Use.Inherited(Use.Default(defaultGraph))(InputPosition.NONE)
  val params: MapValue = MapValue.EMPTY

  def procedures: GlobalProceduresRegistry
  val cypherConfig: CypherConfiguration = CypherConfiguration.fromConfig(Config.defaults())
  val signatures = new SignatureResolver(() => procedures)
  val monitors: Monitors = new Monitors

  val frontend: FabricFrontEnd = FabricFrontEnd(cypherConfig, monitors, signatures)

  def pipeline(query: String): frontend.Pipeline =
    frontend.Pipeline(frontend.preParsing.preParse(query))

  def fragment(query: String): Fragment = {
    val state = pipeline(query).parseAndPrepare.process()
    val fragmenter = new FabricFragmenter(defaultGraphName, query, state.statement(), state.semantics())
    fragmenter.fragment
  }

  def parse(query: String): Statement =
    pipeline(query).parseAndPrepare.process().statement()
}
