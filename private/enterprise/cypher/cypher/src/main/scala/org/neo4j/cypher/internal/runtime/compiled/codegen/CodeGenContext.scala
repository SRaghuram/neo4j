/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.codegen

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.JoinData
import org.neo4j.cypher.internal.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.InternalException

import scala.collection.mutable

case class Variable(name: String, codeGenType: CodeGenType, nullable: Boolean = false)

class CodeGenContext(val semanticTable: SemanticTable,
                     lookup: Map[String, Int], val namer: Namer = Namer()) {

  private val variables: mutable.Map[String, Variable] = mutable.Map()
  private val projectedVariables: mutable.Map[String, Variable] = mutable.Map.empty
  private val probeTables: mutable.Map[CodeGenPlan, JoinData] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()
  val operatorIds: mutable.Map[String, Id] = mutable.Map()

  def addVariable(queryVariable: String, variable: Variable) {
    //assert(!variables.isDefinedAt(queryVariable)) // TODO: Make the cases where overwriting the value is ok explicit (by using updateVariable)
    variables.put(queryVariable, variable)
  }

  def numberOfColumns(): Int = lookup.size

  def nameToIndex(name: String): Int = lookup.getOrElse(name, throw new InternalException(s"$name is not a mapped column"))

  def updateVariable(queryVariable: String, variable: Variable) {
    require(variables.isDefinedAt(queryVariable), s"undefined: $queryVariable")
    variables.put(queryVariable, variable)
  }

  def getVariable(queryVariable: String): Variable = variables(queryVariable)

  def hasVariable(queryVariable: String): Boolean = variables.isDefinedAt(queryVariable)

  def isProjectedVariable(queryVariable: String): Boolean = projectedVariables.contains(queryVariable)

  def variableQueryVariables(): Set[String] = variables.keySet.toSet

  // We need to keep track of variables that are exposed by a QueryHorizon,
  // e.g. Projection, Unwind, ProcedureCall, LoadCsv
  // These variables are the only ones that needs to be considered for materialization by an eager operation, e.g. Sort
  def addProjectedVariable(queryVariable: String, variable: Variable) {
    projectedVariables.put(queryVariable, variable)
  }

  // We need to keep only the projected variables that are exposed by a QueryHorizon, e.g a regular Projection
  def retainProjectedVariables(queryVariablesToRetain: Set[String]): Unit = {
    projectedVariables.retain((key, _) => queryVariablesToRetain.contains(key))
  }

  def getProjectedVariables: Map[String, Variable] = projectedVariables.toMap

  def addProbeTable(plan: CodeGenPlan, codeThunk: JoinData) {
    probeTables.put(plan, codeThunk)
  }

  def getProbeTable(plan: CodeGenPlan): JoinData = probeTables(plan)

  def pushParent(plan: CodeGenPlan) {
    if (plan.isInstanceOf[LeafCodeGenPlan]) {
      throw new IllegalArgumentException(s"Leafs can't be parents: $plan")
    }
    parents.push(plan)
  }

  def popParent(): CodeGenPlan = parents.pop()

  def registerOperator(plan: LogicalPlan): String = {
    val name = namer.newOpName(plan.getClass.getSimpleName)
    operatorIds.put(name, plan.id).foreach(oldId =>
      throw new IllegalStateException(s"Cannot support multiple operators with the same name. Tried to replace operator '$oldId' with '${plan.id}'"))
    name
  }
}
