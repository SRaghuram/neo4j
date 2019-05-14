/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.builder.{AbstractLogicalPlanBuilder, TokenResolver}
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.Variable

/**
  * Test help utility for hand-writing logical queries.
  */
class PhysicalPlanBuilder(breakingPolicy: PipelineBreakingPolicy)
  extends AbstractLogicalPlanBuilder[PhysicalPlan, PhysicalPlanBuilder](new OnDemandTokenContext()) {

  private var semanticTable = new SemanticTable()
  private val tokenContext = tokenResolver.asInstanceOf[TokenContext]

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  def build(readOnly: Boolean = true): PhysicalPlan = {
    val logicalPlan = buildLogicalPlan()
    PhysicalPlanner.plan(tokenContext,
      logicalPlan,
      semanticTable,
      breakingPolicy,
      allocateArgumentSlots = true)
  }
}

class OnDemandTokenContext extends TokenResolver with TokenContext {
  override def getLabelName(id: Int): String = ???

  override def getOptLabelId(labelName: String): Option[Int] = ???

  override def getLabelId(labelName: String): Int = ???

  override def getPropertyKeyName(id: Int): String = ???

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???

  override def getPropertyKeyId(propertyKeyName: String): Int = ???

  override def getRelTypeName(id: Int): String = ???

  override def getOptRelTypeId(relType: String): Option[Int] = ???

  override def getRelTypeId(relType: String): Int = ???
}
