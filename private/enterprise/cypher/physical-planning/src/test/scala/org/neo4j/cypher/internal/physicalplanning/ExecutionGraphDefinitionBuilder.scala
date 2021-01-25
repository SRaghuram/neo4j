/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.mockito.Mockito.mock
import org.neo4j.cypher.internal.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.mutable.ArrayBuffer

class ExecutionGraphDefinitionBuilder(operatorFuserFactory: OperatorFuserFactory[_] = OperatorFuserFactory.NO_FUSION)
  extends AbstractLogicalPlanBuilder[ExecutionGraphDefinition, ExecutionGraphDefinitionBuilder](new NotImplementedTokenContext()) {

  private val plansToBreakOn = ArrayBuffer[Id]()

  private var semanticTable = new SemanticTable()
  private val tokenContext = resolver.asInstanceOf[TokenContext]

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  override def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addTypeInfoCTAny(variable)
  }

  def withBreak(): this.type = {
    plansToBreakOn += idOfLastPlan
    this
  }

  override def build(readOnly: Boolean = true): ExecutionGraphDefinition = {
    val logicalPlan = buildLogicalPlan()
    val breakingPolicy = PipelineBreakingPolicy.breakForIds(plansToBreakOn: _*)
    val leveragedOrders = new LeveragedOrders
    val physicalPlan = PhysicalPlanner.plan(tokenContext,
      logicalPlan,
      semanticTable,
      breakingPolicy,
      CypherRuntimeConfiguration.defaultConfiguration,
      allocateArgumentSlots = true)
    val converters: ExpressionConverters = mock(classOf[ExpressionConverters])
    ExecutionGraphDefiner.defineFrom(breakingPolicy, operatorFuserFactory, physicalPlan, converters, leveragedOrders).executionGraphDefinition
  }
}

class NotImplementedTokenContext extends Resolver with TokenContext {
  override def getLabelName(id: Int): String = ???

  override def getOptLabelId(labelName: String): Option[Int] = ???

  override def getLabelId(labelName: String): Int = ???

  override def getPropertyKeyName(id: Int): String = ???

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = ???

  override def getPropertyKeyId(propertyKeyName: String): Int = ???

  override def getRelTypeName(id: Int): String = ???

  override def getOptRelTypeId(relType: String): Option[Int] = ???

  override def getRelTypeId(relType: String): Int = ???

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???
}
