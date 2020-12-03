/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LockNodesPipe.getNodes
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.DATA_WRITE
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualNodeValue

class LockNodesOperator(val workIdentity: WorkIdentity,
                        val nodesToLock: Set[String]) extends StatelessOperator {

  override def operate(morsel: Morsel,
                       state: PipelinedQueryState,
                       resources: QueryResources): Unit = {
    val cursor = morsel.fullCursor()
    while (cursor.next()) {
      val nodeIds = getNodes(cursor, nodesToLock.toArray)
      state.query.lockNodes(nodeIds: _*)
    }
  }
}

object LockNodesOperator {
  def lockNodes(context: QueryContext, nodesFromLongSlot: Array[Long], nodesFromRefSlots: Array[AnyValue]): Unit = {
    val nodeIds = nodesFromLongSlot ++ nodesFromRefSlots.flatMap {
      case n: VirtualNodeValue => Some(n.id())
      case IsNoValue() => None
      case x: AnyValue => throw CastSupport.typeError[VirtualNodeValue](x)
    }

    context.lockNodes(nodeIds.filterNot(_ == -1L): _*)
  }
}

class LockNodesOperatorTemplate(override val inner: OperatorTaskTemplate,
                                override val id: Id,
                                slotsToLock: Seq[Slot])(protected val codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {
  override def genInit: IntermediateRepresentation = {
    inner.genInit
  }

  override def genOperate: IntermediateRepresentation = {
    require(slotsToLock.filter(_.isLongSlot).forall(_.typ eq CTNode))

    val (nodesFromLongSlots, nodesFromRefsSlots) = slotsToLock.partition(_.isLongSlot)
    block(
      invokeStatic(
        method[LockNodesOperator, Unit, QueryContext, Array[Long], Array[AnyValue]]("lockNodes"),
        DB_ACCESS,
        arrayOf[Long](nodesFromLongSlots.map(_.offset).map(codeGen.getLongAt): _*),
        arrayOf[AnyValue](nodesFromRefsSlots.map(_.offset).map(codeGen.getRefAt): _*),
      ),
      inner.genOperateWithExpressions,
    )
  }

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    inner.genSetExecutionEvent(event)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override def genLocalVariables: Seq[LocalVariable] = Seq.empty

  override def genFields: Seq[Field] = Seq(DATA_WRITE)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override protected def isHead: Boolean = false
}
