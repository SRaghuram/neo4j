/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.runtime.spec.morsel.ParallelStressSuite.{MORSEL_SIZE, WORKERS}
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE_PARALLEL, LogicalQueryBuilder, RuntimeTestSuite, RuntimeTestSupport}
import org.neo4j.cypher.internal.{EnterpriseRuntimeContext, MorselRuntime}
import org.neo4j.graphdb.Node

object ParallelStressSuite {
  val MORSEL_SIZE = 20
  val WORKERS = 20
}

/**
  * Here is a compilation of traits that exert stress on an operator in a given situation. Each trait represents one such situation,
  * and they abstract over the operator to test. The graph used is always the same to make reasoning about expected results easier.
  *
  * To use this, implement a StressTest that extends this class and mixes in all the traits that makes sense, while overriding the required methods.
  */
abstract class ParallelStressSuite() extends RuntimeTestSuite(ENTERPRISE_PARALLEL, MorselRuntime) {

  private val morselsPerGraph = 10
  private val graphSize = morselsPerGraph * MORSEL_SIZE

  /**
    * All nodes in the test definition
    */
  var nodes: Seq[Node] = _

  override def beforeEach(): Unit = {
    graphDb = ENTERPRISE_PARALLEL.graphDatabaseFactory.newImpermanentDatabaseBuilder()
      .setConfig(GraphDatabaseSettings.cypher_morsel_size, MORSEL_SIZE.toString)
      .setConfig(GraphDatabaseSettings.cypher_worker_count, WORKERS.toString)
      .setConfig(GraphDatabaseSettings.cypher_morsel_runtime_scheduler, "lock_free")
      .newGraphDatabase()
    runtimeTestSupport = new RuntimeTestSupport[EnterpriseRuntimeContext](graphDb, ENTERPRISE_PARALLEL)
    initTest()
  }

  implicit class RichLogicalQueryBuilder(inner: LogicalQueryBuilder) {
    def theOperator(op: LogicalQueryBuilder => LogicalQueryBuilder): LogicalQueryBuilder = {
      op(inner)
    }
  }

  def init(): Unit = {
    nodes = nodePropertyGraph(graphSize, {
      case i => Map("prop" -> i)
    }, "Label")
    index("Label", "prop")
    val relTuples = (for (i <- nodes.indices) yield {
      Seq(
        (i, (i + 1) % nodes.length, "NEXT"),
        (i, (i + 2) % nodes.length, "NEXT"),
        (i, (i + 3) % nodes.length, "NEXT"),
        (i, (i + 4) % nodes.length, "NEXT"),
        (i, (i + 5) % nodes.length, "NEXT")
      )
    }).reduce(_ ++ _)
    connect(nodes, relTuples)
  }

  def allNodesNTimes(n: Int): InputValues = {
    inputSingleColumn(morselsPerGraph * n, MORSEL_SIZE, i => nodes(i % nodes.size))
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

    val input: InputValues = allNodesNTimes(10)

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
    * Provide a test definition for the operator. The operator gets
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
      .|.nodeIndexOperator("y:Label(prop < 100)", argumentIds = Set("x", "prop"))
      .projection("x.prop AS prop")
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
  * This tests a leaf operator at the RHS of a Cartesian Product, where the LHS is a parallel Input.
  */
trait RHSOfCartesianLeafStressSuite {
  self: ParallelStressSuite =>

  /**
    * @param operator       a lambda function to append the operator to a query builder.
    * @param expectedResult the expected result for the RHS.
    */
  case class RHSOfCartesianLeafTD(operator: LogicalQueryBuilder => LogicalQueryBuilder, expectedResult: () => Iterable[Array[_]])

  /**
    * Provide a test definition for the operator.
    *
    * @param variable the leaf operator must introduce this variable
    * @return a test definition
    */
  def rhsOfCartesianLeaf(variable: String): RHSOfCartesianLeafTD

  test("should work on RHS of cartesian product with parallelism") {
    // given
    init()

    val input = allNodesNTimes(10)

    val RHSOfCartesianLeafTD(op, expectedResultFromRHS) = rhsOfCartesianLeaf("y")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .cartesianProduct()
      .|.theOperator(op)
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expectedResult = for {
      lhs <- singleNodeInput(input)
      rhs <- expectedResultFromRHS()
    } yield {
      lhs ++ rhs
    }
    runtimeResult should beColumns("x", "y").withRows(expectedResult)
  }

}

/**
  * This tests a one-child operator at the RHS of a Cartesian Product, where the LHS is a parallel Input.
  */
trait RHSOfCartesianOneChildStressSuite {
  self: ParallelStressSuite =>

  /**
    * @param operator       a lambda function to append the operator to a query builder.
    * @param expectedResult the expected result for the RHS, given rows coming into the operator. Each row will contain one value for `y`.
    * @param moreCols       additional columns introduced by the RHS
    */
  case class RHSOfCartesianOneChildTD(operator: LogicalQueryBuilder => LogicalQueryBuilder,
                                      expectedResult: Iterable[Array[Node]] => Iterable[Array[_]],
                                      moreCols: Seq[String] = Seq.empty)

  /**
    * Provide a test definition for the operator.
    *
    * @param variable a node variable available on the RHS
    * @return a test definition
    */
  def rhsOfCartesianOperator(variable: String): RHSOfCartesianOneChildTD

  test("should work on RHS of cartesian product with parallelism") {
    // given
    init()

    val input = allNodesNTimes(10)

    val RHSOfCartesianOneChildTD(op, expectedResult, moreCols) = rhsOfCartesianOperator("y")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x" +: moreCols: _*)
      .cartesianProduct()
      .|.theOperator(op)
      .|.nodeIndexOperator("y:Label(prop < 10)")
      .input(nodes = Seq("x"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val rowsComingIntoTheOperator = for {
      y <- nodes.slice(0, 10)
    } yield {
      Array(y)
    }
    val rowsFromRHS = expectedResult(rowsComingIntoTheOperator)
    val expectedWholeResult = for {
      lhsRow <- singleNodeInput(input)
      rhsRow <- rowsFromRHS
    } yield lhsRow ++ rhsRow
    runtimeResult should beColumns("x" +: moreCols: _*).withRows(expectedWholeResult)
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
    * Provide a test definition for the operator. The operator gets
    *
    * @param variable a node variable available
    * @return a test definition
    */
  def onTopOfParallelInputOperator(variable: String): OnTopOfParallelInputTD

  test("should work on top of input with parallelism") {
    // given
    init()

    val input = allNodesNTimes(10)

    val OnTopOfParallelInputTD(op, expectedResult, resultColumns) = onTopOfParallelInputOperator("x")

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

// TODO
// * ProduceResults immediately following a reduce
// * ProduceResults immediately following a streaming

