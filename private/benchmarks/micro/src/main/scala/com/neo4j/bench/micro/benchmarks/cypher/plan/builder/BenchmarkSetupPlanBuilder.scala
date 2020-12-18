/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cypher.plan.builder

import com.neo4j.bench.micro.benchmarks.cypher.TestSetup
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.LeveragedOrders
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.symbols.CypherType

import scala.collection.mutable.ArrayBuffer

// TODO, create a default implementation for LogicalPlanBuilder https://trello.com/c/3j5PQ19Z/17-implementation-of-abstractlogicalplanbuilder-that-does-not-depend-on-planner
class BenchmarkSetupPlanBuilder(wholePlan: Boolean = true, resolver: Resolver = new LogicalPlanResolver) extends AbstractLogicalPlanBuilder[TestSetup, BenchmarkSetupPlanBuilder](resolver, wholePlan) {

  val cardinalities: Cardinalities = new Cardinalities with Default[LogicalPlan, Cardinality] {
    override protected def defaultValue: Cardinality = Cardinality.SINGLE
  }

  val providedOrders: ProvidedOrders = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
    override protected def defaultValue: ProvidedOrder = ProvidedOrder.empty
  }

  private var semanticTable = new SemanticTable()

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  override def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addTypeInfoCTAny(variable)
  }

  def getSemanticTable: SemanticTable = semanticTable

  def withCardinality(x: Double): BenchmarkSetupPlanBuilder = {
    cardinalities.set(idOfLastPlan, Cardinality(x))
    this
  }

  def withProvidedOrder(order: ProvidedOrder): BenchmarkSetupPlanBuilder = {
    providedOrders.set(idOfLastPlan, order)
    this
  }

  def newVar(name: String, typ: CypherType): BenchmarkSetupPlanBuilder = {
    newVar(name, pos, typ)
  }

  def newVar(name: String, inputPosition: InputPosition, typ: CypherType): BenchmarkSetupPlanBuilder = {
    val variable = Variable(name)(inputPosition)
    semanticTable = semanticTable.copy(types = semanticTable.types.updated(variable, ExpressionTypeInfo(typ.invariant, None)))
    this
  }

  def build(readOnly: Boolean = true): TestSetup = {
    val plan = buildLogicalPlan()
    TestSetup(
      plan,
      semanticTable,
      plan.asInstanceOf[ProduceResult].columns.toList,
      new LeveragedOrders,
      cardinalities,
      providedOrders,
      idGen
    )
  }
}

// Duplicate of org.neo4j.cypher.internal.compiler.helpers.LogicalPlanResolver
// TODO expose the original LogicalPlanResolver to be able to reuse here https://trello.com/c/3j5PQ19Z/17-implementation-of-abstractlogicalplanbuilder-that-does-not-depend-on-planner
class LogicalPlanResolver(
  labels: ArrayBuffer[String] = new ArrayBuffer[String](),
  properties: ArrayBuffer[String] = new ArrayBuffer[String](),
  relTypes: ArrayBuffer[String] = new ArrayBuffer[String]()
) extends Resolver with TokenContext {

  override def getLabelId(label: String): Int = {
    val index = labels.indexOf(label)
    if (index == -1) {
      labels += label
      labels.size - 1
    } else {
      index
    }
  }

  override def getPropertyKeyId(prop: String): Int = {
    val index = properties.indexOf(prop)
    if (index == -1) {
      properties += prop
      properties.size - 1
    } else {
      index
    }
  }

  override def getRelTypeId(relType: String): Int = {
    val index = relTypes.indexOf(relType)
    if (index == -1) {
      relTypes += relType
      relTypes.size - 1
    } else {
      index
    }
  }

  override def getLabelName(id: Int): String = if (id >= labels.size) throw new IllegalStateException(s"Label $id undefined") else labels(id)

  override def getOptLabelId(labelName: String): Option[Int] = Some(getLabelId(labelName))

  override def getPropertyKeyName(id: Int): String = if (id >= properties.size) throw new IllegalStateException(s"Property $id undefined") else properties(id)

  override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = Some(getPropertyKeyId(propertyKeyName))

  override def getRelTypeName(id: Int): String = if (id >= relTypes.size) throw new IllegalStateException(s"RelType $id undefined") else relTypes(id)

  override def getOptRelTypeId(relType: String): Option[Int] = Some(getRelTypeId(relType))

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = ???
}