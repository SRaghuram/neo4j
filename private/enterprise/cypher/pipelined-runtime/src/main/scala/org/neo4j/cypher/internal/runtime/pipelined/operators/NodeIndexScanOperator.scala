/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.{SlotConfiguration, SlottedIndexedProperty}
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.pipelined.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.pipelined.{NodeIndexCursorRepresentation, OperatorExpressionCompiler}
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.{IndexReadSession, KernelReadTracer, NodeValueIndexCursor}
import org.neo4j.internal.schema.IndexOrder
import org.neo4j.values.storable.Value

class NodeIndexScanOperator(val workIdentity: WorkIdentity,
                            nodeOffset: Int,
                            properties: Array[SlottedIndexedProperty],
                            queryIndexId: Int,
                            indexOrder: IndexOrder,
                            argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperatorWithValues[NodeValueIndexCursor](nodeOffset, properties) {

  private val needsValues: Boolean = indexPropertyIndices.nonEmpty

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    val indexSession = state.queryIndexes(queryIndexId)
    IndexedSeq(new OTask(inputMorsel.nextCopy, indexSession))
  }

  class OTask(val inputMorsel: MorselExecutionContext, index: IndexReadSession) extends InputLoopTask {

    override def workIdentity: WorkIdentity = NodeIndexScanOperator.this.workIdentity

    private var cursor: NodeValueIndexCursor = _

    protected override def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {

      cursor = resources.cursorPools.nodeValueIndexCursorPool.allocateAndTrace()
      val read = context.transactionalContext.dataRead
      read.nodeIndexScan(index, cursor, indexOrder, needsValues)
      true
    }

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      iterate(inputMorsel, outputRow, cursor, argumentSize)
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      if (cursor != null) {
        cursor.setTracer(event)
      }
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      resources.cursorPools.nodeValueIndexCursorPool.free(cursor)
      cursor = null
    }
  }
}

class NodeIndexScanTaskTemplate(inner: OperatorTaskTemplate,
                                id: Id,
                                innermost: DelegateOperatorTaskTemplate,
                                nodeVarName: String,
                                offset: Int,
                                properties: Array[SlottedIndexedProperty],
                                queryIndexId: Int,
                                indexOrder: IndexOrder,
                                argumentSize: SlotConfiguration.Size)
                               (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val nodeIndexCursorField = field[NodeValueIndexCursor](codeGen.namer.nextVariableName())
  private val needsValues = properties.exists(_.getValueFromIndex)

  codeGen.registerCursor(nodeVarName, NodeIndexCursorRepresentation(loadField(nodeIndexCursorField)))

  override def genMoreFields: Seq[Field] = Seq(nodeIndexCursorField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation =
    block(
      condition(isNotNull(loadField(nodeIndexCursorField)))(
        invokeSideEffect(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Unit, KernelReadTracer]("setTracer"), event)
        ),
      inner.genSetExecutionEvent(event)
      )

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {
        /**
          * {{{
          *   this.nodeIndexCursor = resources.cursorPools.nodeValuIndexCursorPool.allocate()
          *   context.transactionalContext.dataRead.nodeIndexScan(session, cursor, indexOrder, needsValues)
          *   val tmp = nodeCursor.next()
          *   profileRow(tmp)
          *   this.canContinue = tmp
          *   true
          * }}}
          */
        block(
          allocateAndTraceCursor(nodeIndexCursorField, executionEventField, ALLOCATE_NODE_INDEX_CURSOR),
          nodeIndexScan(indexReadSession(queryIndexId), loadField(nodeIndexCursorField), indexOrder, needsValues),
          setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id)),
          constant(true)
          )
    }

  override protected def genInnerLoop: IntermediateRepresentation = {
    val indexPropertyIndices: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
    val indexPropertySlotOffsets: Array[Int] = properties.flatMap(_.maybeCachedNodePropertySlot)

    val cacheProperties = for (i <- indexPropertyIndices.indices) yield
      codeGen.setCachedPropertyAt(indexPropertySlotOffsets(i),
                                  invoke(loadField(nodeIndexCursorField),
                                         method[NodeValueIndexCursor, Value, Int]("propertyValue"),
                                         constant(indexPropertyIndices(i))))
    /**
      * {{{
      *   while (hasDemand && this.canContinue) {
      *     setLongAt(offset, nodeIndexCursor.nodeReference())
      *     setCachedPropertyAt(cacheOffset1, nodeIndexCursor.propertyValue(0))
      *     setCachedPropertyAt(cacheOffset2, nodeIndexCursor.propertyValue(1))
      *     ...
      *     << inner.genOperate >>
      *     val tmp = nodeCursor.next()
      *     profileRow(tmp)
      *     this.canContinue = tmp      *   }
      * }}}
      */
    loop(and(innermost.predicate, loadField(canContinue)))(
      block(
        codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
        codeGen.setLongAt(offset, invoke(loadField(nodeIndexCursorField), method[NodeValueIndexCursor, Long]("nodeReference"))),
        block(cacheProperties:_*),
        inner.genOperateWithExpressions,
        doIfInnerCantContinue(setField(canContinue, profilingCursorNext[NodeValueIndexCursor](loadField(nodeIndexCursorField), id))),
        endInnerLoop
        )
      )
  }


  override protected def genCloseInnerLoop: IntermediateRepresentation = {
    /**
      * {{{
      *   resources.cursorPools.nodeValueIndexCursorPool.free(nodeIndexCursor)
      *   nodeIndexCursor = null
      * }}}
      */
    block(
      freeCursor[NodeValueIndexCursor](loadField(nodeIndexCursorField), NodeValueIndexCursorPool),
      setField(nodeIndexCursorField, constant(null))
      )
  }
}

