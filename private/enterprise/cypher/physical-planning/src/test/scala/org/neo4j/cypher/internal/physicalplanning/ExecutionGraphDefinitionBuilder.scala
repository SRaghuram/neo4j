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
import org.neo4j.cypher.internal.v4_0.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

class ExecutionGraphDefinitionBuilder()
  extends AbstractLogicalPlanBuilder[ExecutionGraphDefinition, ExecutionGraphDefinitionBuilder](new NotImplementedTokenContext()) {

  private val plansToBreakOn = ArrayBuffer[Id]()

  private var semanticTable = new SemanticTable()
  private val tokenContext = tokenResolver.asInstanceOf[TokenContext]

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  def withBreak(): this.type = {
    plansToBreakOn += idOfLastPlan
    this
  }

  def build(readOnly: Boolean = true): ExecutionGraphDefinition = {
    val logicalPlan = buildLogicalPlan()
    val breakingPolicy = PipelineBreakingPolicy.breakForIds(plansToBreakOn: _*)
    val physicalPlan = PhysicalPlanner.plan(tokenContext,
      logicalPlan,
      semanticTable,
      breakingPolicy,
      allocateArgumentSlots = true)
    PipelineBuilder.build(breakingPolicy, physicalPlan)
  }
}

class NotImplementedTokenContext extends TokenResolver with TokenContext {
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
