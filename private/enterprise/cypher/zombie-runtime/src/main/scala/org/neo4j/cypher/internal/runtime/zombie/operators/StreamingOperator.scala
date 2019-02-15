/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.zombie.ArgumentStateCreator
import org.neo4j.cypher.internal.runtime.morsel._
import org.neo4j.cypher.internal.runtime.scheduling.HasWorkIdentity
import org.neo4j.cypher.internal.runtime.zombie.state.MorselParallelizer

/**
  * A executable morsel operator.
  */
trait Operator extends HasWorkIdentity {

  /**
    * Create a new execution state for this operator.
    *
    * @param argumentStateCreator creator used to construct a argumentStateMap for this operator state
    * @return the new execution state for this operator.
    */
  def createState(argumentStateCreator: ArgumentStateCreator): OperatorState
}

/**
  * The execution state of an operator. One instance of this is create for every query execution.
  */
trait OperatorState {

  /**
    * Initialize new tasks for this operator. This code path let's operators create
    * multiple output rows for each row in `inputMorsel`.
    */
  def init(context: QueryContext,
           state: QueryState,
           inputMorsel: MorselParallelizer,
           resources: QueryResources): IndexedSeq[ContinuableInputOperatorTask]
}

/**
  * A streaming operator is initialized with an input morsel, to produce 0-n [[ContinuableOperatorTask]].
  */
trait StreamingOperator extends Operator with OperatorState {
  override def init(context: QueryContext,
                    state: QueryState,
                    inputMorsel: MorselParallelizer,
                    resources: QueryResources): IndexedSeq[ContinuableInputOperatorTask]

  override final def createState(argumentStateCreator: ArgumentStateCreator): OperatorState = this
}

/**
  * A continuable operator is initialized to produce exactly one [[ContinuableOperatorTask]].
  */
trait ContinuableOperator extends HasWorkIdentity {
  def init(context: QueryContext,
           state: QueryState,
           resources: QueryResources): ContinuableOperatorTask
}

/**
  * Physical immutable operator. Thread-safe. In contrast to [[StreamingOperator]], [[StatelessOperator]]
  * has no init-method to generate a task, but performs it's logic directly in the [[OperatorTask#operate]] call.
  */
trait StatelessOperator extends OperatorTask with HasWorkIdentity

/**
  * Operator related task.
  */
trait OperatorTask {
  def operate(output: MorselExecutionContext,
              context: QueryContext,
              state: QueryState,
              resources: QueryResources): Unit
}

/**
  * Operator task which might require several operate calls to be fully executed.
  */
trait ContinuableOperatorTask extends OperatorTask {
  def canContinue: Boolean
}

/**
  * Operator task which might require several operate calls to be fully executed, and is computing over an input morsel.
  */
trait ContinuableInputOperatorTask extends ContinuableOperatorTask {
  val inputMorsel: MorselExecutionContext
}
