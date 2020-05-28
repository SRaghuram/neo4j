/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.stress

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.stress.ParallelStressSuite.MORSEL_SIZE
import org.neo4j.cypher.internal.runtime.spec.stress.ParallelStressSuite.WORKERS
import org.neo4j.graphdb.Node

object ParallelStressSuite {
  val MORSEL_SIZE = 20
  val WORKERS = 20
}

/**
 * Here is a compilation of traits that exert stress on an operator in a given situation. Each trait represents one such situation,
 * and they abstract over the operator to test. The graph used is always the same to make reasoning about expected results easier.
 *
 * To use this, implement a StressTest that extends this class and mixes in all the traits that make sense, while overriding the required methods.
 */
abstract class ParallelStressSuite(edition: Edition[EnterpriseRuntimeContext], runtime: CypherRuntime[EnterpriseRuntimeContext])
  extends RuntimeTestSuite(
    edition.copyWith(
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_small -> Integer.valueOf(MORSEL_SIZE),
      GraphDatabaseInternalSettings.cypher_pipelined_batch_size_big -> Integer.valueOf(MORSEL_SIZE),
      GraphDatabaseInternalSettings.cypher_worker_count -> Integer.valueOf(WORKERS)),
    runtime) {

  private val morselsPerGraph = 10
  protected val graphSize: Int = morselsPerGraph * MORSEL_SIZE

  /**
   * All nodes in the test definition
   */
  var nodes: Seq[Node] = _

  implicit class RichLogicalQueryBuilder(inner: LogicalQueryBuilder) {
    def theOperator(op: LogicalQueryBuilder => LogicalQueryBuilder): LogicalQueryBuilder = {
      op(inner)
    }
  }

  /**
   * This can be used to investigate flaky tests.
   */
  def stringify(thread: Thread, elements: Array[StackTraceElement]): Unit = {
    val builder = new StringBuilder("\"" + thread.getName + "\"" + (if (thread.isDaemon) {
      " daemon"
    } else {
      ""
    }) + " prio=" + thread.getPriority + " tid=" + thread.getId + " " + thread.getState.name.toLowerCase + "\n")
    builder.append("   ").append(classOf[Thread.State].getName).append(": ").append(thread.getState.name.toUpperCase).append("\n")
    for (element <- elements) {
      builder.append("      at ").append(element.getClassName).append(".").append(element.getMethodName)
      if (element.isNativeMethod) {
        builder.append("(Native method)")
      } else if (element.getFileName == null) {
        builder.append("(Unknown source)")
      } else {
        builder.append("(").append(element.getFileName).append(":").append(element.getLineNumber).append(")")
      }
      builder.append("\n")
    }
    println(builder.toString)
  }

  def init(): Unit = {
    nodes = given {
      try {
        index("Label", "prop")
        index("Label", "text")
        index("Label", "propWithDuplicates")
      } catch {
        case e: IllegalStateException =>
          // TODO This is to investigate flaky tests
          Thread.getAllStackTraces.forEach {
            case (thread, trace) => stringify(thread, trace)
          }
          throw e
      }
      val ns = nodePropertyGraph(graphSize, {
        case i => Map("prop" -> i, "text" -> i.toString, "propWithDuplicates" -> ((i / 2) * 2))
      }, "Label")
      val relTuples = (for (i <- ns.indices) yield {
        Seq(
          (i, (i + 1) % ns.length, "NEXT"),
          (i, (i + 2) % ns.length, "NEXT"),
          (i, (i + 3) % ns.length, "NEXT"),
          (i, (i + 4) % ns.length, "NEXT"),
          (i, (i + 5) % ns.length, "NEXT")
        )
      }).reduce(_ ++ _)
      connect(ns, relTuples)
      ns
    }
  }

  def allNodesNTimes(n: Int): InputValues = {
    inputColumns(morselsPerGraph * n, MORSEL_SIZE, i => nodes(i % nodes.size))
  }

  def singleNodeInput(input: InputValues): Iterable[Array[Node]] = input.flatten.map(_.map(_.asInstanceOf[Node]))
}

/**
 * This tests a leaf operator at the RHS of an Apply, where the LHS is a parallel Input.
 */
trait RHSOfApplyLeafStressSuite {
  self: ParallelStressSuite =>

  /**
   * @param operator       a lambda function to append the operator to a query builder.
   * @param expectedResult the expected result for the RHS, given rows coming into the operator. Each row will contain one value for `x`.
   */
  case class RHSOfApplyLeafTD(operator: LogicalQueryBuilder => LogicalQueryBuilder, expectedResult: Iterable[Array[Node]] => Iterable[Array[_]])

  /**
   * Provide a test definition for the operator.
   *
   * @param variable     the leaf operator must introduce this variable
   * @param nodeArgument it gets provided with this argument, which is a node
   * @param propArgument it gets provided with this argument, which is a integer property of the node
   * @return a test definition
   */
  def rhsOfApplyLeaf(variable: String, nodeArgument: String, propArgument: String): RHSOfApplyLeafTD

  test("should work on RHS of apply with parallelism") {
    // given
    init()

    val input: InputValues = allNodesNTimes(2)

    val RHSOfApplyLeafTD(op, expectedResult) = rhsOfApplyLeaf("y", "x", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.theOperator(op)
      .projection("x.prop AS prop")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val rowsComingIntoTheOperator = singleNodeInput(input)
    runtimeResult should beColumns("x", "y").withRows(expectedResult(rowsComingIntoTheOperator))
  }
}

/**
 * This tests a one-child operator at the RHS of an Apply, where the LHS is a parallel Input.
 */
trait RHSOfApplyOneChildStressSuite {
  self: ParallelStressSuite =>

  /**
   * @param operator       a lambda function to append the operator to a query builder.
   * @param expectedResult the expected result for the RHS, given rows coming into the operator. Each row will contain one value for `x` and `y`.
   * @param resultColumns  all result columns
   */
  case class RHSOfApplyOneChildTD(operator: LogicalQueryBuilder => LogicalQueryBuilder,
                                  expectedResult: Iterable[Array[Node]] => Iterable[Array[_]],
                                  resultColumns: Seq[String] = Seq.empty)

  /**
   * Provide a test definition for the operator.
   *
   * @param variable a node variable available on the RHS
   * @return a test definition
   */
  def rhsOfApplyOperator(variable: String): RHSOfApplyOneChildTD

  test("should work on RHS of apply with parallelism") {
    // given
    init()

    val input = allNodesNTimes(1)

    val RHSOfApplyOneChildTD(op, expectedResult, resultColumns) = rhsOfApplyOperator("y")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumns: _*)
      .apply()
      .|.theOperator(op)
      .|.nodeIndexOperator("y:Label(propWithDuplicates < 100)", argumentIds = Set("x", "prop"))
      .projection("x.propWithDuplicates AS prop")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then

    val rowsComingIntoTheOperator = for {
      Array(x) <- singleNodeInput(input)
      y <- nodes.slice(0, 100)
    } yield {
      Array(x, y)
    }
    runtimeResult should beColumns(resultColumns: _*).withRows(expectedResult(rowsComingIntoTheOperator))
  }
}

/**
 * This tests a one-child operator on top of parallel Input.
 */
trait OnTopOfParallelInputStressTest {
  self: ParallelStressSuite =>

  /**
   * @param operator       a lambda function to append the operator to a query builder.
   * @param expectedResult the expected result given rows coming into the operator. Each row will contain one value for `x`.
   * @param resultColumns  all result columns
   */
  case class OnTopOfParallelInputTD(operator: LogicalQueryBuilder => LogicalQueryBuilder,
                                    expectedResult: Iterable[Array[Node]] => Iterable[Array[_]],
                                    resultColumns: Seq[String] = Seq.empty)

  /**
   * Provide a test definition for the operator.
   *
   * @param variable a node variable available
   * @param propVariable an integer property of the node
   * @return a test definition
   */
  def onTopOfParallelInputOperator(variable: String, propVariable: String): OnTopOfParallelInputTD

  test("should work on top of input with parallelism") {
    // given
    init()

    val input = allNodesNTimes(10)

    val OnTopOfParallelInputTD(op, expectedResult, resultColumns) = onTopOfParallelInputOperator("x", "prop")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumns: _*)
      .theOperator(op)
      .projection("x.prop AS prop")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then

    val rowsComingIntoTheOperator = singleNodeInput(input)
    runtimeResult should beColumns(resultColumns: _*).withRows(expectedResult(rowsComingIntoTheOperator))
  }
}
