/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.operators

import org.neo4j.codegen.api.Field
import org.neo4j.codegen.api.IntermediateRepresentation
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.declareAndAssign
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeSideEffect
import org.neo4j.codegen.api.IntermediateRepresentation.isNotNull
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.LocalVariable
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.physicalplanning.ArgumentStateMapId
import org.neo4j.cypher.internal.physicalplanning.BufferId
import org.neo4j.cypher.internal.profiling.OperatorProfileEvent
import org.neo4j.cypher.internal.runtime.compiled.expressions.ExpressionCompilation.nullCheckIfRequired
import org.neo4j.cypher.internal.runtime.compiled.expressions.IntermediateExpression
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.pipelined.ArgumentStateMapCreator
import org.neo4j.cypher.internal.runtime.pipelined.ExecutionState
import org.neo4j.cypher.internal.runtime.pipelined.OperatorExpressionCompiler
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.Aggregators
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.AvgDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectAllAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CollectDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.CountStarAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.IsEmptyAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MaxAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.MinAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.NonEmptyAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.PercentileContAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.PercentileDiscAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevPAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.StdevPDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.SumAggregator
import org.neo4j.cypher.internal.runtime.pipelined.aggregators.SumDistinctAggregator
import org.neo4j.cypher.internal.runtime.pipelined.execution.ArgumentSlots
import org.neo4j.cypher.internal.runtime.pipelined.execution.Morsel
import org.neo4j.cypher.internal.runtime.pipelined.execution.PipelinedQueryState
import org.neo4j.cypher.internal.runtime.pipelined.execution.QueryResources
import org.neo4j.cypher.internal.runtime.pipelined.operators.AggregationMapperOperatorTaskTemplate.createAggregators
import org.neo4j.cypher.internal.runtime.pipelined.operators.OperatorCodeGenHelperTemplates.argumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRow
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRowMap
import org.neo4j.cypher.internal.runtime.pipelined.state.AggregatedRowUpdaters
import org.neo4j.cypher.internal.runtime.pipelined.state.ArgumentStateMap
import org.neo4j.cypher.internal.runtime.pipelined.state.Collections.singletonIndexedSeq
import org.neo4j.cypher.internal.runtime.pipelined.state.StateFactory
import org.neo4j.cypher.internal.runtime.scheduling.WorkIdentity
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.SyntaxException
import org.neo4j.values.AnyValue

/**
 * General purpose aggregation operator, supporting clauses like
 *
 * {{{
 *   WITH key1, key2, key3, sum(..) AS aggr1, count(..) AS aggr2, avg(..) AS aggr3
 * }}}
 *
 * The implementation composes an [[AggregationMapperOperator]], an [[AggregatedRowMap]] and an [[AggregationReduceOperator]].
 */
case class AggregationOperator(workIdentity: WorkIdentity,
                               aggregations: Array[Aggregator],
                               groupings: GroupingExpression) {
  def mapper(argumentSlotOffset: Int,
             argumentStateMapId: ArgumentStateMapId,
             expressionValues: Array[Expression],
             operatorId: Id) =
    new SingleArgumentAggregationMapperOperator(argumentSlotOffset, argumentStateMapId, expressionValues)(operatorId)

  def mapper(argumentSlotOffset: Int,
             argumentStateMapId: ArgumentStateMapId,
             expressionValues: Array[Array[Expression]],
             operatorId: Id) =
    new AggregationMapperOperator(argumentSlotOffset, argumentStateMapId, expressionValues)(operatorId)

  def reducer(argumentStateMapId: ArgumentStateMapId,
              reducerOutputSlots: Array[Int],
              operatorId: Id) =
    new AggregationReduceOperator(argumentStateMapId, reducerOutputSlots)(operatorId)

  /**
   * Pre-operator for aggregations with grouping. This performs local aggregation of the
   * data in a single morsel at a time, before putting these local aggregations into the
   * [[ExecutionState]] buffer which perform the final global aggregation.
   */
  class SingleArgumentAggregationMapperOperator(argumentSlotOffset: Int,
                                  argumentStateMapId: ArgumentStateMapId,
                                  expressionValues: Array[Expression])
                                 (val id: Id = Id.INVALID_ID) extends OutputOperator {
    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity
    override def outputBuffer: Option[BufferId] = None
    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      new State(executionState.argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[AggregatedRowMap]])
    }

    class State(aggregationMaps: ArgumentStateMap[AggregatedRowMap]) extends OutputOperatorState {

      override def trackTime: Boolean = true

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {
        val queryState = state.queryStateForExpressionEvaluation(resources)

        var argumentRowId = -1L
        var aggregationMap: AggregatedRowMap = null
        val readCursor = morsel.readCursor()
        while (readCursor.next()) {
          val arg = ArgumentSlots.getArgumentAt(readCursor, argumentSlotOffset)
          if (arg != argumentRowId) {
            if (aggregationMap != null) {
              aggregationMap.applyUpdates(resources.workerId)
            }
            aggregationMap = aggregationMaps.peek(arg)
            argumentRowId = arg
          }

          val groupingValue = groupings.computeGroupingKey(readCursor, queryState)
          val aggregatorRow = aggregationMap.get(groupingValue)
          val update = aggregatorRow.updaters(resources.workerId)
          var i = 0
          while (i < aggregations.length) {
            val value = expressionValues(i)(readCursor, queryState)
            update.addUpdate(i, value)
            i += 1
          }
        }

        if (aggregationMap != null) {
          aggregationMap.applyUpdates(resources.workerId)
        }

        NoOutputOperator
      }
    }
  }


  class AggregationMapperOperator(argumentSlotOffset: Int,
                                  argumentStateMapId: ArgumentStateMapId,
                                  expressionValues: Array[Array[Expression]])
                                 (val id: Id = Id.INVALID_ID)
    extends OutputOperator {

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def outputBuffer: Option[BufferId] = None

    override def createState(executionState: ExecutionState, stateFactory: StateFactory): OutputOperatorState = {
      new State(executionState.argumentStateMaps(argumentStateMapId).asInstanceOf[ArgumentStateMap[AggregatedRowMap]])
    }

    class State(aggregationMaps: ArgumentStateMap[AggregatedRowMap]) extends OutputOperatorState {

      override def trackTime: Boolean = true

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      override def prepareOutput(morsel: Morsel,
                                 state: PipelinedQueryState,
                                 resources: QueryResources,
                                 operatorExecutionEvent: OperatorProfileEvent): PreparedOutput = {
        val queryState = state.queryStateForExpressionEvaluation(resources)

        var argumentRowId = -1L
        var aggregationMap: AggregatedRowMap = null
        val readCursor = morsel.readCursor()
        while (readCursor.next()) {
          val arg = ArgumentSlots.getArgumentAt(readCursor, argumentSlotOffset)
          if (arg != argumentRowId) {
            if (aggregationMap != null) {
              aggregationMap.applyUpdates(resources.workerId)
            }
            aggregationMap = aggregationMaps.peek(arg)
            argumentRowId = arg
          }

          val groupingValue = groupings.computeGroupingKey(readCursor, queryState)
          val aggregatorRow = aggregationMap.get(groupingValue)
          val update = aggregatorRow.updaters(resources.workerId)
          var i = 0
          while (i < aggregations.length) {
            val arguments = expressionValues(i)
            val input = new Array[AnyValue](arguments.length)
            var j = 0
            while (j < arguments.length) {
              input(j) = arguments(j)(readCursor, queryState)
              j += 1
            }
            update.addUpdate(i, input)
            i += 1
          }
        }

        if (aggregationMap != null) {
          aggregationMap.applyUpdates(resources.workerId)
        }

        NoOutputOperator
      }
    }
  }

  /**
   * Operator which streams aggregated data, built by [[AggregationMapperOperator]] and [[AggregatedRowMap]].
   */
  class AggregationReduceOperator(val argumentStateMapId: ArgumentStateMapId,
                                  reducerOutputSlots: Array[Int])
                                 (val id: Id = Id.INVALID_ID)
    extends Operator
    with AccumulatorsInputOperatorState[AnyRef, AggregatedRowMap] {

    override def accumulatorsPerTask(morselSize: Int): Int = 1

    override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

    override def createState(argumentStateCreator: ArgumentStateMapCreator,
                             stateFactory: StateFactory,
                             state: PipelinedQueryState,
                             resources: QueryResources): AccumulatorsInputOperatorState[AnyRef, AggregatedRowMap] = {
      val memoryTracker = stateFactory.newMemoryTracker(id.x)
      argumentStateCreator.createArgumentStateMap(argumentStateMapId, new AggregatedRowMap.Factory(aggregations, memoryTracker, state.numberOfWorkers))
      this
    }

    override def nextTasks(input: IndexedSeq[AggregatedRowMap],
                           resources: QueryResources): IndexedSeq[ContinuableOperatorTaskWithAccumulators[AnyRef, AggregatedRowMap]] = {
      singletonIndexedSeq(new OTask(input))
    }

    class OTask(override val accumulators: IndexedSeq[AggregatedRowMap])
      extends ContinuableOperatorTaskWithAccumulators[AnyRef, AggregatedRowMap] {

      AssertMacros.checkOnlyWhenAssertionsAreEnabled(accumulators.size == 1)
      private val accumulator = accumulators.head

      override def workIdentity: WorkIdentity = AggregationOperator.this.workIdentity

      private val resultIterator = accumulator.result()

      override def operate(outputMorsel: Morsel,
                           state: PipelinedQueryState,
                           resources: QueryResources): Unit = {

        val outputCursor = outputMorsel.fullCursor(onFirstRow = true)
        while (resultIterator.hasNext && outputCursor.onValidRow()) {
          val entry = resultIterator.next() // NOTE: This entry is transient and only valid until we call next() again
          val key = entry.getKey
          val aggregators = entry.getValue

          outputCursor.copyFrom(accumulator.argumentRow)
          groupings.project(outputCursor, key.asInstanceOf[groupings.KeyType])
          var i = 0
          while (i < aggregations.length) {
            outputCursor.setRefAt(reducerOutputSlots(i), aggregators.result(i))
            i += 1
          }
          outputCursor.next()
        }
        outputCursor.truncate()
      }

      override def canContinue: Boolean = resultIterator.hasNext
    }
  }
}

object AggregationMapperOperatorTaskTemplate {
  def createAggregators(aggregators: Array[Aggregator]): IntermediateRepresentation = {
    val newAggregators = aggregators.map {
      case CountStarAggregator => getStatic[Aggregators,Aggregator]("COUNT_STAR")
      case CountAggregator => getStatic[Aggregators,Aggregator]("COUNT")
      case CountDistinctAggregator => getStatic[Aggregators,Aggregator]("COUNT_DISTINCT")
      case SumAggregator => getStatic[Aggregators,Aggregator]("SUM")
      case SumDistinctAggregator => getStatic[Aggregators,Aggregator]("SUM_DISTINCT")
      case AvgAggregator => getStatic[Aggregators,Aggregator]("AVG")
      case AvgDistinctAggregator => getStatic[Aggregators,Aggregator]("AVG_DISTINCT")
      case MaxAggregator => getStatic[Aggregators,Aggregator]("MAX")
      case MinAggregator => getStatic[Aggregators,Aggregator]("MIN")
      case CollectAggregator => getStatic[Aggregators,Aggregator]("COLLECT")
      case CollectAllAggregator => getStatic[Aggregators,Aggregator]("COLLECT_ALL")
      case CollectDistinctAggregator => getStatic[Aggregators,Aggregator]("COLLECT_DISTINCT")
      case NonEmptyAggregator => getStatic[Aggregators,Aggregator]("NON_EMPTY")
      case IsEmptyAggregator => getStatic[Aggregators,Aggregator]("IS_EMPTY")
      case StdevAggregator => getStatic[Aggregators,Aggregator]("STDEV")
      case StdevDistinctAggregator => getStatic[Aggregators,Aggregator]("STDEV_DISTINCT")
      case StdevPAggregator => getStatic[Aggregators,Aggregator]("STDEVP")
      case StdevPDistinctAggregator => getStatic[Aggregators,Aggregator]("STDEVP_DISTINCT")
      case PercentileContAggregator => getStatic[Aggregators,Aggregator]("PERCENTILE_CONT")
      case PercentileDiscAggregator => getStatic[Aggregators,Aggregator]("PERCENTILE_DISC")
      case aggregator =>
        throw new SyntaxException(s"Unexpected Aggregator: ${aggregator.getClass.getName}")
    }
    arrayOf[Aggregator](newAggregators: _ *)
  }
}

abstract class BaseAggregationMapperOperatorTaskTemplate(val inner: OperatorTaskTemplate,
                                            override val id: Id,
                                            argumentSlotOffset: Int,
                                            aggregators: Array[Aggregator],
                                            argumentStateMapId: ArgumentStateMapId,
                                            groupingKeyExpressionCreator: () => IntermediateExpression,
                                            serialExecutionOnly: Boolean,
                                            codeGen: OperatorExpressionCompiler) extends OperatorTaskTemplate {

  override def toString: String = "AggregationMapperOperatorTaskTemplate"

  private val needToApplyUpdates = !Aggregator.allDirect(aggregators) || !serialExecutionOnly

  private val asmField: Field =
    field[ArgumentStateMap[AggregatedRowMap]](codeGen.namer.nextVariableName("aggregatedRowMaps"),
      argumentStateMap[AggregatedRowMap](argumentStateMapId))

  private val aggregatorsVar = variable[Array[Aggregator]](codeGen.namer.nextVariableName("aggregators"), createAggregators(aggregators))
  private val argVar = variable[Long](codeGen.namer.nextVariableName("arg"), constant(-1L))
  private val aggregatedRowMapVar = variable[AggregatedRowMap](codeGen.namer.nextVariableName("aggregatedRowMap"), constant(null))
  private val workerIdVar = variable[Int](codeGen.namer.nextVariableName("workerId"), constant(-1))

  private var compiledGroupingExpression: IntermediateExpression = _

  protected def setupAggregation(): Unit
  protected def addUpdates(updaters: IntermediateRepresentation): IntermediateRepresentation
  protected def genMoreExpressions: Seq[IntermediateExpression]

  // constructor
  override def genInit: IntermediateRepresentation = inner.genInit

  override def genOperateEnter: IntermediateRepresentation = {
    block(
      assign(workerIdVar, invoke(OperatorCodeGenHelperTemplates.QUERY_RESOURCES, method[QueryResources, Int]("workerId"))),
      super.genOperateEnter
    )
  }

  // this operates on a single row only
  override def genOperate: IntermediateRepresentation = {
    setupAggregation()
    if (null == compiledGroupingExpression) {
      compiledGroupingExpression = groupingKeyExpressionCreator()
    }

    /**
     * // last seen argument
     * long arg = -1
     *
     * // map of grouping value -> aggregatedRow
     * val aggregatedRowMap = null
     *
     * {{{
     *
     *   // ----- track when argument changes -----
     *
     *   val currentArg = getFromLongSlot(argumentSlotOffset)
     *   if (currentArg != arg) {
     *     arg = currentArg
     *     if (aggregatedRowMap != null) {
     *       aggregatedRowMap.applyUpdates()
     *     }
     *     aggregatedRowMap = aggregatedRowMaps.peek(arg)
     *   }
     *
     *   // ----- create updaters group for each new grouping -----
     *
     *   val groupingValue: AnyValue = {compiledGroupingExpression.ir}
     *   val aggregatedRow: AggregatedRow = aggregatedRowMap.get(groupingValue)
     *   val updaters: AggregatedRowUpdaters = aggregatedRow.updaters(workerId)
     *
     *
     *   // ----- aggregate -----
     *
     *   {
     *     updaters.addUpdate(0, aggregationExpression[0]())
     *     updaters.addUpdate(1, aggregationExpression[1]())
     *     ...
     *     updaters.addUpdate(n-1, aggregationExpression[n-1]())
     *   }
     * }}}
     *
     */

    // argument of current morsel row
    val currentArg = codeGen.namer.nextVariableName("currentArg")
    val groupingValue = codeGen.namer.nextVariableName("groupingValue")
    val aggregatedRow = codeGen.namer.nextVariableName("aggregatedRow")
    val updaters = codeGen.namer.nextVariableName("updaters")

    block(

      /*
       * val arg = ArgumentSlots.getArgumentAt(readCursor, argumentSlotOffset)
       * if (arg != argumentRowId) {
       *   if (aggregatedRowMap != null) {           // ---
       *     aggregatedRowMap.applyUpdates(workerId) // If all aggregators are direct we skip this code
       *   }                                         // ---
       *   aggregatedRowMap = aggregatedRowMaps.peek(arg)
       *   argumentRowId = arg
       * }
       */
      declareAndAssign(typeRefOf[Long], currentArg, codeGen.getArgumentAt(argumentSlotOffset)),
      condition(notEqual(load(currentArg), load(argVar)))(
        block(
          assign(argVar, load(currentArg)),
          genApplyUpdates,
          assign(aggregatedRowMapVar, OperatorCodeGenHelperTemplates.peekState[AggregatedRowMap](loadField(asmField), load(argVar)))
        )),

      /*
       * val groupingValue: AnyValue = {compiledGroupingExpression.ir}
       * val aggregatedRow: AggregatedRow = aggregatedRowMap.get(groupingValue)
       * val updaters: AggregatedRowUpdaters = aggregatedRow.updaters(workerId)
       */
      declareAndAssign(typeRefOf[AnyValue],
        groupingValue,
        compiledGroupingExpression.ir),

      declareAndAssign(typeRefOf[AggregatedRow],
        aggregatedRow,
        invoke(load(aggregatedRowMapVar), method[AggregatedRowMap, AggregatedRow, AnyValue]("get"), load(groupingValue))),

      declareAndAssign(typeRefOf[AggregatedRowUpdaters],
        updaters,
        invoke(load(aggregatedRow), method[AggregatedRow, AggregatedRowUpdaters, Int]("updaters"), load(workerIdVar))),

      /*
       * updaters.addUpdate(0, aggregationExpression[0]())
       * updaters.addUpdate(1, aggregationExpression[1]())
       * ...
       * updaters.addUpdate(n-1, aggregationExpression[n-1]())
       */
      addUpdates(load(updaters)),
      inner.genOperateWithExpressions
    )
  }

  override def genOperateExit: IntermediateRepresentation = {
    block(
      genApplyUpdates,
      super.genOperateExit
    )
  }

  private def genApplyUpdates: IntermediateRepresentation = {
    if (needToApplyUpdates) {
      condition(isNotNull(load(aggregatedRowMapVar)))(
        invokeSideEffect(load(aggregatedRowMapVar), method[AggregatedRowMap, Unit, Int]("applyUpdates"), load(workerIdVar))
      )
    } else {
      noop()
    }
  }
  override def genExpressions: Seq[IntermediateExpression] = genMoreExpressions ++ Seq(compiledGroupingExpression)

  override def genOutputBuffer: Option[IntermediateRepresentation] = None

  override def genFields: Seq[Field] = Seq(asmField)

  override def genLocalVariables: Seq[LocalVariable] = Seq(argVar, aggregatorsVar, aggregatedRowMapVar, workerIdVar)

  override def genCanContinue: Option[IntermediateRepresentation] = inner.genCanContinue

  override def genCloseCursors: IntermediateRepresentation = inner.genCloseCursors

  override def genSetExecutionEvent(event: IntermediateRepresentation): IntermediateRepresentation = inner.genSetExecutionEvent(event)
}

class SingleArgumentAggregationMapperOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                            id: Id,
                                            argumentSlotOffset: Int,
                                            aggregators: Array[Aggregator],
                                            argumentStateMapId: ArgumentStateMapId,
                                            aggregationExpressionsCreator : () => Array[IntermediateExpression],
                                            groupingKeyExpressionCreator: () => IntermediateExpression,
                                            serialExecutionOnly: Boolean = false)
                                           (protected val codeGen: OperatorExpressionCompiler)
  extends BaseAggregationMapperOperatorTaskTemplate(inner, id, argumentSlotOffset, aggregators, argumentStateMapId, groupingKeyExpressionCreator, serialExecutionOnly, codeGen) {

  private var compiledAggregationExpressions: Array[IntermediateExpression] = _

  override protected def setupAggregation(): Unit = if (null == compiledAggregationExpressions) {
    compiledAggregationExpressions = aggregationExpressionsCreator()
  }

  override protected def addUpdates(updaters: IntermediateRepresentation): IntermediateRepresentation = block(
    compiledAggregationExpressions.indices.map(i => {
      invokeSideEffect(updaters, method[AggregatedRowUpdaters, Unit, Int, AnyValue]("addUpdate"),
        constant(i),
        nullCheckIfRequired(compiledAggregationExpressions(i)))
    }): _ *
  )
  override def genMoreExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions.toSeq
}

class AggregationMapperOperatorTaskTemplate(inner: OperatorTaskTemplate,
                                            id: Id,
                                            argumentSlotOffset: Int,
                                            aggregators: Array[Aggregator],
                                            argumentStateMapId: ArgumentStateMapId,
                                            aggregationExpressionsCreator : () => Array[Array[IntermediateExpression]],
                                            groupingKeyExpressionCreator: () => IntermediateExpression,
                                            serialExecutionOnly: Boolean = false)
                                           (protected val codeGen: OperatorExpressionCompiler)
  extends BaseAggregationMapperOperatorTaskTemplate(inner, id, argumentSlotOffset, aggregators, argumentStateMapId, groupingKeyExpressionCreator, serialExecutionOnly, codeGen) {

  private var compiledAggregationExpressions: Array[Array[IntermediateExpression]] = _

  override protected def setupAggregation(): Unit = if (null == compiledAggregationExpressions) {
    compiledAggregationExpressions = aggregationExpressionsCreator()
  }

  override protected def addUpdates(updaters: IntermediateRepresentation): IntermediateRepresentation = block(
    compiledAggregationExpressions.indices.map(i => {
      invokeSideEffect(updaters, method[AggregatedRowUpdaters, Unit, Int, Array[AnyValue]]("addUpdate"),
        constant(i),
        arrayOf[AnyValue](compiledAggregationExpressions(i).map(e => nullCheckIfRequired(e)):_*))
    }): _ *
  )
  override def genMoreExpressions: Seq[IntermediateExpression] = compiledAggregationExpressions.flatten.toSeq
}
