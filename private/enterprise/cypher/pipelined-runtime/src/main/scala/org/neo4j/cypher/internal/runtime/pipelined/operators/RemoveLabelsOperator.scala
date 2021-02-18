/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.ArrayLiteral
import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.pipelined.MutableQueryStatistics
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.LabelRemover.labelNamesToIds
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATE
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.QUERY_STATS_TRACKER_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.getNodeIdFromSlot
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

class RemoveLabelsOperator(
  val workIdentity: WorkIdentity,
  private[this] val nodeVariable: String,
  private[this] val removeLabels: Array[String]
) extends StatelessOperator {
  private[this] var profileEvent: OperatorProfileEvent = _
  private[this] var labelIds: Array[Int] = _ // Avoids reading label ids on each invocation of operate

  override def operate(output: Morsel, state: PipelinedQueryState, resources: QueryResources): Unit = {
    val cursor = output.readCursor()
    if (labelIds == null) {
      labelIds = labelNamesToIds(state, profileEvent, removeLabels.toArray)
    }
    val labelRemover = new LabelRemover(labelIds, state.queryContext, resources.queryStatisticsTracker, profileEvent)

    while(cursor.next()) {
      cursor.getByName(nodeVariable) match {
        case node: VirtualNodeValue => labelRemover.removeLabels(node.id())
        case Values.NO_VALUE => // Do nothing
        case unexpectedValue => throw new CypherTypeException(s"Expected node but got ${unexpectedValue.getClass.getSimpleName}")
      }
    }
  }

  override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
    profileEvent = event
  }
}

class RemoveLabelsTemplate(
  override val inner: OperatorTaskTemplate,
  override val id: Id,
  nodeSlot: Slot,
  removeLabels: Seq[String]
)(
  protected val codeGen: OperatorExpressionCompiler
) extends OperatorTaskTemplate {

  // Put label ids in a field to avoid having to resolve ids on each invocation to operate
  private val removeLabelIdsField = field[Array[Int]](codeGen.namer.nextVariableName("removeLabelIds"))

  private val labelRemoverVar: LocalVariable = {
    variable[LabelRemover](
      codeGen.namer.nextVariableName("labelRemover"),
      block(
        condition(isNull(loadField(removeLabelIdsField))) {
          setField(
            removeLabelIdsField,
            invokeStatic(
              method[LabelRemover, Array[Int], QueryState, OperatorProfileEvent, Array[String]]("labelNamesToIds"),
              QUERY_STATE,
              loadField(executionEventField),
              ArrayLiteral(typeRefOf[String], removeLabels.map(constant))
            )
          )
        },
        newInstance(
          constructor[LabelRemover, Array[Int], QueryContext, MutableQueryStatistics, OperatorProfileEvent],
          loadField(removeLabelIdsField),
          invoke(QUERY_STATE, method[QueryState, QueryContext]("query")),
          QUERY_STATS_TRACKER,
          loadField(executionEventField)
        )
      )
    )
  }

  private val nodeIdVariableName = codeGen.namer.nextVariableName("removeLabelsNodeId")

  override protected def isHead: Boolean = false

  override def genInit: IntermediateRepresentation = inner.genInit

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = {
    inner.genSetExecutionEvent(event)
  }

  override protected def genOperate: IntermediateRepresentation = {
    block(
      declareAndAssign(nodeIdVariableName, getNodeIdFromSlot(nodeSlot, codeGen)),
      condition(notEqual(nodeIdVariableName, constant(StatementConstants.NO_SUCH_NODE))) {
        invokeSideEffect(load(labelRemoverVar), method[LabelRemover, Unit, Long]("removeLabels"), load[Long](nodeIdVariableName))
      },
      inner.genOperateWithExpressions,
      conditionallyProfileRow(innerCannotContinue, id, doProfile)
    )
  }

  override def genExpressions: Seq[IntermediateExpression] = Seq()

  override def genFields: Seq[Field] = Seq(removeLabelIdsField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(QUERY_STATS_TRACKER_V, labelRemoverVar)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors
}

class LabelRemover(
  private[this] val removeLabelIds: Array[Int],
  private[this] val context: QueryContext,
  private[this] val stats: MutableQueryStatistics,
  private[this] val profileEvent: OperatorProfileEvent
) {
  def removeLabels(nodeId: Long): Unit = {
    val removeCount = context.removeLabelsFromNode(nodeId, removeLabelIds.iterator)
    stats.removeLabels(removeCount)
    if (profileEvent != null) {
      profileEvent.dbHit()
    }
  }
}

object LabelRemover {
  def labelNamesToIds(state: QueryState, profileEvent: OperatorProfileEvent, labelNames: Array[String]): Array[Int] = {
    val labelIds = labelNames.map(state.query.nodeLabel)
    if (profileEvent != null) {
      profileEvent.dbHits(labelIds.size)
    }
    labelIds
  }
}
