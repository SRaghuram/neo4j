/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.codegen.api.IntermediateRepresentation._
import org.neo4j.codegen.api.{Field, IntermediateRepresentation, LocalVariable}
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.morsel.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.morsel.execution.{MorselExecutionContext, QueryResources, QueryState}
import org.neo4j.cypher.internal.runtime.morsel.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.morsel.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.runtime.{DbAccess, ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.NameId
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.util.Preconditions
import org.neo4j.values.storable.{LongValue, Values}


class NodeCountFromCountStoreOperator(val workIdentity: WorkIdentity,
                                      offset: Int,
                                      labels: Seq[Option[LazyLabel]],
                                      argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  private val lazyLabels: Array[LazyLabel] = labels.flatten.toArray
  private val wildCards: Int = labels.count(_.isEmpty)

  override protected def nextTasks(queryContext: QueryContext,
                                   state: QueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    IndexedSeq(new NodeFromCountStoreTask(inputMorsel.nextCopy))
  }


  class NodeFromCountStoreTask(val inputMorsel: MorselExecutionContext) extends InputLoopTask {

    override def toString: String = "NodeFromCountStoreTask"

    private var hasNext = false
    private var executionEvent: OperatorProfileEvent = OperatorProfileEvent.NONE

    override protected def initializeInnerLoop(context: QueryContext,
                                               state: QueryState,
                                               resources: QueryResources,
                                               initExecutionContext: ExecutionContext): Boolean = {
      hasNext = true
      true
    }

    override def workIdentity: WorkIdentity = NodeCountFromCountStoreOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselExecutionContext, context: QueryContext, state: QueryState): Unit = {
      if (hasNext) {
        var count = 1L

        var i = 0
        while (i < lazyLabels.length) {
          val idOfLabel = lazyLabels(i).getId(context)
          if (idOfLabel == LazyLabel.UNKNOWN) {
            count = 0
          } else {
            executionEvent.dbHit()
            count = count * context.nodeCountByCountStore(idOfLabel)
          }
          i += 1
        }
        if (wildCards > 0) {
          i = 0
          val wildCardCount = context.nodeCountByCountStore(NameId.WILDCARD)
          executionEvent.dbHit()
          while (i < wildCards) {
            count *= wildCardCount
            i += 1
          }
        }

        outputRow.copyFrom(inputMorsel, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setRefAt(offset, Values.longValue(count))
        outputRow.moveToNextRow()
        hasNext = false
      }
    }

    override def setExecutionEvent(event: OperatorProfileEvent): Unit = {
      this.executionEvent = event
    }

    override protected def closeInnerLoop(resources: QueryResources): Unit = {
      // nothing to do here
    }
  }

}

class NodeCountFromCountStoreOperatorTemplate(override val inner: OperatorTaskTemplate,
                                              id: Id,
                                              innermost: DelegateOperatorTaskTemplate,
                                              offset: Int,
                                              labels: List[Option[Either[String, Int]]],
                                              argumentSize: SlotConfiguration.Size)
                                             (codeGen: OperatorExpressionCompiler)
  extends InputLoopTaskTemplate(inner, id, innermost, codeGen) {

  import OperatorCodeGenHelperTemplates._

  private val wildCards: Int = labels.count(_.isEmpty)
  private val knownLabels = labels.flatten.collect {
    case Right(token) => token
  }
  Preconditions.checkState(knownLabels.forall(_ >= 0), "Expect all label tokens to be positive")

  private val nodeLabelCursorField = field[NodeLabelIndexCursor](codeGen.namer.nextVariableName())
  private val labelFields = labels.flatten.collect {
    case Left(labelName) => labelName -> field[Int](codeGen.namer.variableName(labelName), NO_TOKEN)
  }.toMap

  override def genMoreFields: Seq[Field] = labelFields.values.toSeq :+ nodeLabelCursorField

  override def genLocalVariables: Seq[LocalVariable] = Seq(CURSOR_POOL_V)

  override def genExpressions: Seq[IntermediateExpression] = Seq.empty

  override protected def genInitializeInnerLoop: IntermediateRepresentation = {

    val setLabelIds = labelFields.toSeq.map{
      case (name, field) => condition(equal(loadField(field), NO_TOKEN))(setField(field, nodeLabelId(name)))
    }
    block(setLabelIds ++
            Seq(setField(canContinue, constant(true)),
            constant(true)):_*)
  }

  /**
    *{{{
    *  var count = 1L
    *  //for the labels not known at compile time
    *  if (this.labelField1 == -1) {
    *    count = 0L
    *  } else {
    *    count = count * dbAccess.nodeCountByCountStore(this.labelField1)
    *  }
    *  if (this.labelField2 == -1) {
    *      count = 0L
    *    } else {
    *      count = count * dbAccess.nodeCountByCountStore(this.labelField2)
    *  }
    *  ...
    *  //for labels known at compile time
    *  count = count * dbAccess.nodeCountByCountStore(11)
    *  count = count * dbAccess.nodeCountByCountStore(1337)
    *  ...
    *  //for wild card labels
    *  val wildCardCount = dbAccess.nodeCountByCountStore(-1)
    *  count = count * wildCardCount
    *  count = count * wildCardCount
    *  count = count * wildCardCount
    *  ...
    *  setRefAt(offset, Values.longValue(count))
    *  << inner.genOperate >>
    *  this.canContinue = false
    *}}}
    */
  override protected def genInnerLoop: IntermediateRepresentation = {
    val countVar = codeGen.namer.nextVariableName()
    //takes care of the labels we don't know at compile time
    val unknownLabelOps = block(labelFields.values.toSeq.map(field => {
      ifElse(equal(loadField(field), constant(-1))){
        assign(countVar, constant(0L))
      }{
        block(
          assign(countVar, multiply(load(countVar), invoke(DB_ACCESS, method[DbAccess, Long, Int]("nodeCountByCountStore"), loadField(field)))),
          invoke(loadField(executionEventField), TRACE_DB_HIT))
      }
    }):_*)

    //takes care of the labels we do know at compile time
    val knownLabelOps = block(knownLabels.map(token =>
        assign(countVar, multiply(load(countVar), invoke(DB_ACCESS, method[DbAccess, Long, Int]("nodeCountByCountStore"), constant(token))))) :+
        invoke(loadField(executionEventField), TRACE_DB_HITS, constant(knownLabels.size)) :_*)

    //take care of all wildcard labels
    val wildCardOps = if (wildCards > 0) {
      val wildCardCount = codeGen.namer.nextVariableName()
      val ops = block((1 to wildCards).map(_ => assign(countVar, multiply(load(countVar), load(wildCardCount)))) :_*)
      block(
        declareAndAssign(typeRefOf[Long], wildCardCount, invoke(DB_ACCESS, method[DbAccess, Long, Int]("nodeCountByCountStore"), constant(NameId.WILDCARD))),
        invoke(loadField(executionEventField), TRACE_DB_HIT),
        ops
      )
    } else noop()

    block(
      declareAndAssign(typeRefOf[Long], countVar, constant(1L)),
      unknownLabelOps,
      knownLabelOps,
      wildCardOps,
      codeGen.copyFromInput(argumentSize.nLongs, argumentSize.nReferences),
      codeGen.setRefAt(offset, invokeStatic(method[Values, LongValue, Long]("longValue"), load(countVar))),
      profileRow(id),
      inner.genOperateWithExpressions,
      setField(canContinue, constant(false))
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()
}

