/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.equal
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.multiply
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.ReadWriteRow
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.DB_ACCESS
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.MorselFullCursor
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.CURSOR_POOL_V
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.NO_TOKEN
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.conditionallyProfileRow
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.dbHit
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.dbHits
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.nodeLabelId
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap.ArgumentStateMaps
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.MorselParallelizer
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.NameId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor
import org.neo4j.util.Preconditions
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values

class NodeCountFromCountStoreOperator(val workIdentity: WorkIdentity,
                                      offset: Int,
                                      labels: Seq[Option[LazyLabel]],
                                      argumentSize: SlotConfiguration.Size) extends StreamingOperator {

  private val lazyLabels: Array[LazyLabel] = labels.flatten.toArray
  private val wildCards: Int = labels.count(_.isEmpty)

  override protected def nextTasks(state: PipelinedQueryState,
                                   inputMorsel: MorselParallelizer,
                                   parallelism: Int,
                                   resources: QueryResources,
                                   argumentStateMaps: ArgumentStateMaps): IndexedSeq[ContinuableOperatorTaskWithMorsel] = {
    singletonIndexedSeq(new NodeFromCountStoreTask(inputMorsel.nextCopy))
  }


  class NodeFromCountStoreTask(inputMorsel: Morsel) extends InputLoopTask(inputMorsel) {

    override def toString: String = "NodeFromCountStoreTask"

    private var hasNext = false
    private var executionEvent: OperatorProfileEvent = _

    override protected def initializeInnerLoop(state: PipelinedQueryState, resources: QueryResources, initExecutionContext: ReadWriteRow): Boolean = {
      hasNext = true
      true
    }

    override def workIdentity: WorkIdentity = NodeCountFromCountStoreOperator.this.workIdentity

    override protected def innerLoop(outputRow: MorselFullCursor, state: PipelinedQueryState): Unit = {
      if (hasNext) {
        var count = 1L

        var i = 0
        while (i < lazyLabels.length) {
          val idOfLabel = lazyLabels(i).getId(state.queryContext)
          if (idOfLabel == LazyLabel.UNKNOWN) {
            count = 0
          } else {
            if (executionEvent != null) {
              executionEvent.dbHit()
            }
            count = count * state.queryContext.nodeCountByCountStore(idOfLabel)
          }
          i += 1
        }
        if (wildCards > 0) {
          i = 0
          val wildCardCount = state.queryContext.nodeCountByCountStore(NameId.WILDCARD)
          if (executionEvent != null) {
            executionEvent.dbHit()
          }
          while (i < wildCards) {
            count *= wildCardCount
            i += 1
          }
        }

        outputRow.copyFrom(inputCursor, argumentSize.nLongs, argumentSize.nReferences)
        outputRow.setRefAt(offset, Values.longValue(count))
        outputRow.next()
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
          dbHit(loadField(executionEventField)))
      }
    }):_*)

    //takes care of the labels we do know at compile time
    val knownLabelOps = block(knownLabels.map(token =>
      assign(countVar, multiply(load(countVar), invoke(DB_ACCESS, method[DbAccess, Long, Int]("nodeCountByCountStore"), constant(token))))) :+
      dbHits(loadField(executionEventField), constant(knownLabels.size.toLong)) :_*)

    //take care of all wildcard labels
    val wildCardOps = if (wildCards > 0) {
      val wildCardCount = codeGen.namer.nextVariableName()
      val ops = block((1 to wildCards).map(_ => assign(countVar, multiply(load(countVar), load(wildCardCount)))) :_*)
      block(
        declareAndAssign(typeRefOf[Long], wildCardCount, invoke(DB_ACCESS, method[DbAccess, Long, Int]("nodeCountByCountStore"), constant(NameId.WILDCARD))),
        dbHit(loadField(executionEventField)),
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
      inner.genOperateWithExpressions,
      setField(canContinue, constant(false)),
      conditionallyProfileRow(innerCantContinue, id)
    )
  }

  override protected def genCloseInnerLoop: IntermediateRepresentation = noop()

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = noop()
}

