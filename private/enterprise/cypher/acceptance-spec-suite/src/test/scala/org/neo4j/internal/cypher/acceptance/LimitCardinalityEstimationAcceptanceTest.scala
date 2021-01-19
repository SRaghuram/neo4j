/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.VOID
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.values.AnyValue

import scala.math.round

class LimitCardinalityEstimationAcceptanceTest extends ExecutionEngineFunSuite with CreateTempFileTestSupport with CypherComparisonSupport {

  test("should estimate rows when parameterized") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p LIMIT $limit",
      params = Map("limit" -> 10))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows when literal") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p LIMIT 10")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(10)
  }

  test("should estimate rows when literal and query has parameters") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) WHERE 50 = $fifty RETURN p LIMIT 10",
      params = Map("fifty" -> 50))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(10)
  }

  test("should estimate rows when independent expression without parameters") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInteger(ceil(cos(0))) + 4")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(5)
  }

  test("should estimate rows by default value when expression contains parameter") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 as x, p RETURN p LIMIT toInteger(sin($limit))",
      params = Map("limit" -> 1))
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows by default value when expression contains rand()") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    // NOTE: We cannot executeWith because of random result
    val result = executeSingle("EXPLAIN MATCH (p:Person) with 10 AS x, p RETURN p LIMIT toInteger(rand()*10)", Map.empty)
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows by default value when expression contains timestamp()") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val r1 = executeSingle("EXPLAIN MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r1.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)

    val r2 = executeSingle("EXPLAIN CYPHER runtime=slotted MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r2.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)

    val r3 = executeSingle("EXPLAIN CYPHER runtime=interpreted MATCH (p:Person) with 10 AS x, p RETURN p LIMIT timestamp()")
    r3.executionPlanDescription() should
      includeSomewhere.aPlan("Limit").withEstimatedRows(PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt)
  }

  test("should estimate rows after DISTINCT") {
    (0 until 100).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN DISTINCT p LIMIT 17")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(17)
  }

  test("should estimate rows after aggregation") {
    (0 until 25*25).map(_ => createLabeledNode("Person"))
    val result = executeSingle("EXPLAIN MATCH (p:Person) RETURN p, count(*) LIMIT 17")
    result.executionPlanDescription() should
      includeSomewhere.aPlan("Limit")
        .withEstimatedRows(17)
  }

  test("should estimate rows for skip + limit") {
    val nodeCount = 100
    (0 until nodeCount).foreach(_ => createNode())

    final case class TestCase(query: String, skip: Int, limit: Int)
    val defaultLimit = PlannerDefaults.DEFAULT_LIMIT_ROW_COUNT
    val defaultSkip = PlannerDefaults.DEFAULT_SKIP_ROW_COUNT

    Seq(
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT 42", // SKIP auto-parameterized
        skip = 42, limit = 42 + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT 500", // SKIP auto-parameterized
        skip = nodeCount - defaultSkip, limit = nodeCount),
      TestCase("MATCH (n) RETURN n SKIP 333 LIMIT 444", // SKIP auto-parameterized
        skip = nodeCount - defaultSkip, limit = nodeCount),
      TestCase("MATCH (n) RETURN n SKIP 12345 LIMIT 17", // SKIP auto-parameterized
        skip = 17, limit = 17 + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP $skip LIMIT $limit",
        skip = defaultLimit, limit = defaultLimit + defaultSkip),
      TestCase("MATCH (n) RETURN n SKIP 17 LIMIT $limit",
        skip = defaultLimit, limit = defaultLimit + 17),
      TestCase("MATCH (n) RETURN n, $prop SKIP 42 LIMIT 17",
        skip = 17, limit = 17 + 42),
      TestCase("MATCH (n) RETURN n, $prop SKIP 444 LIMIT 333",
        skip = 0, limit = nodeCount),
      TestCase("MATCH (n) RETURN n, $prop SKIP 0 LIMIT 17",
        skip = 17, limit = 17),
      TestCase("MATCH (n) RETURN n, $prop SKIP 0 LIMIT 0",
        skip = 0, limit = 0),
    ) foreach { t =>
      withClue(t) {
        val result = executeSingle(s"EXPLAIN ${t.query}", Map("prop" -> "hack", "skip" -> 17, "limit" -> 23))
        result.executionPlanDescription() should {
          includeSomewhere.aPlan("Skip").withEstimatedRows(t.skip) onTopOf
            aPlan("Limit").withEstimatedRows(t.limit)
        }
      }
    }

  }

  test("should estimate rows with selectivity in children") {
    val nodeCount = 100
    val limit = 10
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) RETURN n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("AllNodesScan").withEstimatedRows(limit))
      )
  }

  test("should estimate rows with selectivity through CartesianProduct") {
    val nodeCount = 100
    val limit = 10
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) RETURN n, m LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("CartesianProduct").withEstimatedRows(limit)
          .withChildren(
            aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(limit))),
            aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(limit)))
          )
        )
      )
  }

  test("should estimate rows with selectivity through NodeHashJoin") {
    val nodeCount = 20
    val limit = 5
    (0 until nodeCount / 2).foreach(i => executeSingle(s"CREATE ({ind: $i})-[:REL]->({ind: ${i + nodeCount / 2}})"))

    val result = executeSingle(s"MATCH (n)--(m) USING JOIN ON m RETURN m LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("NodeHashJoin").withEstimatedRows(limit)
          .withChildren(
            aPlan("AllNodesScan").withEstimatedRows(nodeCount),
            aPlan("Expand(All)").withEstimatedRows(limit)
              .withLHS(aPlan("AllNodesScan").withEstimatedRows(limit))
          )
        )
      )
  }

  test("should estimate rows with reset selectivity through exhaustive limit") {
    val nodeCount = 100
    val limit = 50
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) CREATE (m:M) RETURN n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("ExhaustiveLimit").withEstimatedRows(limit)
        .withLHS(aPlan("Create").withEstimatedRows(nodeCount)
          .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount))
        )
      )
  }

  test("should estimate rows with reset selectivity through sort") {
    val nodeCount = 5000
    val limit = 49
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"PROFILE MATCH (n), (m) WITH n, m ORDER BY m RETURN n, m LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("CartesianProduct").withEstimatedRows(limit)
          .withChildren(
            aPlan("Sort").withEstimatedRows(round(math.sqrt(limit)))
              .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount)),
            aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(limit)))
          )
        )
      )
  }

  test("should estimate rows with reset selectivity through top") {
    val nodeCount = 100
    val limit = 50
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) RETURN n, m ORDER BY n, m LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Top").withEstimatedRows(limit)
        .withLHS(aPlan("CartesianProduct").withEstimatedRows(nodeCount * nodeCount)
          .withChildren(
            aPlan("AllNodesScan").withEstimatedRows(nodeCount),
            aPlan("AllNodesScan").withEstimatedRows(nodeCount)
          )
        )
      )
  }

  test("should estimate rows with limit selectivity through partial top") {
    val nodeCount = 100
    val limit = 5
    (0 until nodeCount).foreach(i => createLabeledNode(Map("idx" -> i), "Person"))
    graph.createIndex("Person", "idx")

    val result = executeSingle(s"MATCH (n:Person) WHERE exists(n.idx) RETURN n.idx, n.b ORDER BY n.idx, n.b LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("PartialTop").withEstimatedRows(limit)
          .withLHS(aPlan("Projection").withEstimatedRows(limit)
              .withLHS(aPlan("NodeIndexScan").withEstimatedRows(limit))
          )
      )
  }

  test("should estimate rows with reset selectivity through partial top and limit") {
    val nodeCount = 100
    val highLimit = 10
    val lowLimit = 5
    (0 until nodeCount).foreach(i => createLabeledNode(Map("idx" -> 1), "Person"))
    graph.createIndex("Person", "idx")

    val result = executeSingle(s"MATCH (n:Person) WHERE exists(n.idx) WITH n ORDER BY n.idx, n.b LIMIT $highLimit RETURN * LIMIT $lowLimit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit)
      .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
        .withLHS(aPlan("PartialTop").withEstimatedRows(lowLimit)
          .withLHS(aPlan("Projection").withEstimatedRows(lowLimit)
            .withLHS(aPlan("NodeIndexScan").withEstimatedRows(lowLimit))
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is earliest") {
    val nodeCount = 100
    val lowLimit = 9
    val highLimit = 20
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $lowLimit RETURN n, m LIMIT $highLimit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit)
      .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
        .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
          .withLHS(aPlan("CartesianProduct").withEstimatedRows(lowLimit)
              .withChildren(
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit))),
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit)))
              )
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is latest") {
    val nodeCount = 100
    val lowLimit = 5
    val highLimit = 200000
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $highLimit RETURN n, m LIMIT $lowLimit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit)
      .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
        .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
          .withLHS(aPlan("CartesianProduct").withEstimatedRows(lowLimit)
              .withChildren(
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit))),
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit)))
              )
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is latest and cardinality is increased between limits") {
    val nodeCount = 100
    val lowLimit = 500
    val highLimit = 200000
    val unwindFactor = 100
    (0 until nodeCount).foreach(_ => createNode())

    // Param so that unwind does not get auto-parameterized.
    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $highLimit UNWIND range(1, $unwindFactor) AS i RETURN n, m, $$x LIMIT $lowLimit", Map("x" -> 10))

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit)
      .withLHS(includeSomewhere.aPlan("Limit").withEstimatedRows(lowLimit)
        .withLHS(aPlan("Unwind").withEstimatedRows(lowLimit)
          .withLHS(aPlan("Limit").withEstimatedRows(round(lowLimit / unwindFactor.toDouble))
            .withLHS(aPlan("CartesianProduct").withEstimatedRows(round(lowLimit / unwindFactor.toDouble))
              .withChildren(
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit / unwindFactor.toDouble))),
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit / unwindFactor.toDouble)))
              )
            )
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is latest and cardinality is decreased between limits") {
    val nodeCount = 100
    val lowLimit = 25
    val highLimit = 200000
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $highLimit WHERE n.prop > 5 RETURN n, m LIMIT $lowLimit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit)
      .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
        .withLHS(aPlan("Filter").withEstimatedRows(lowLimit)
          .withLHS(aPlan("Limit").withEstimatedRows(round((lowLimit / PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor)))
            .withLHS(aPlan("CartesianProduct").withEstimatedRows(round((lowLimit / PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor)))
              .withChildren(
                includeSomewhere.aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit / PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor))),
                includeSomewhere.aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit / PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor)))
              )
            )
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is earliest and cardinality is increased between limits") {
    val nodeCount = 100
    val lowLimit = 5
    val highLimit = 200000
    val unwindFactor = 100
    (0 until nodeCount).foreach(_ => createNode())

    // Param so that unwind does not get auto-parameterized.
    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $lowLimit UNWIND range(1, $unwindFactor) AS i RETURN n, m, $$x LIMIT $highLimit", Map("x" -> 10))

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(lowLimit * unwindFactor)
      .withLHS(includeSomewhere.aPlan("Limit").withEstimatedRows(lowLimit * unwindFactor)
        .withLHS(aPlan("Unwind").withEstimatedRows(lowLimit * unwindFactor)
          .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
            .withLHS(aPlan("CartesianProduct").withEstimatedRows(lowLimit)
              .withChildren(
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit))),
                aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit)))
              )
            )
          )
        )
      )
  }

  test("should estimate rows with lowest limit selectivity when lowest limit is earliest and cardinality is decreased between limits") {
    val nodeCount = 100
    val lowLimit = 25
    val highLimit = 200000
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n), (m) WITH n, m LIMIT $lowLimit WHERE n.prop > 5 RETURN n, m LIMIT $highLimit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(round(lowLimit * PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor))
      .withLHS(aPlan("Limit").withEstimatedRows(round(lowLimit * PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor))
        .withLHS(aPlan("Filter").withEstimatedRows(round(lowLimit * PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor))
          .withLHS(aPlan("Limit").withEstimatedRows(lowLimit)
            .withLHS(aPlan("CartesianProduct").withEstimatedRows(lowLimit)
              .withChildren(
                includeSomewhere.aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit))),
                includeSomewhere.aPlan("AllNodesScan").withEstimatedRows(round(math.sqrt(lowLimit)))
              )
            )
          )
        )
      )
  }

  test("should estimate rows with limit selectivity through Distinct") {
    val nodeCount = 100
    val limit = 20
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) RETURN DISTINCT n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("Distinct").withEstimatedRows(limit)
          .withLHS(aPlan("AllNodesScan").withEstimatedRows(round(limit / PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY.factor)))
        )
      )
  }

  test("should estimate rows with limit selectivity through OrderedDistinct") {
    val nodeCount = 100
    val limit = 20
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) WITH n ORDER BY n RETURN DISTINCT n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("OrderedDistinct").withEstimatedRows(limit)
          .withLHS(aPlan("Sort").withEstimatedRows(round(limit / PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY.factor))
            .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount))
          )
        )
      )
  }

  test("should estimate rows with reset selectivity through Aggregation") {
    val nodeCount = 500
    val limit = 20
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) RETURN n, collect(n) LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("EagerAggregation").withEstimatedRows(limit)
          .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount))
        )
      )
  }

  test("should estimate rows with limit selectivity through OrderedAggregation") {
    val nodeCount = 100
    val limit = 5
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) WITH n ORDER BY n RETURN n, collect(n) LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("OrderedAggregation").withEstimatedRows(limit)
          .withLHS(aPlan("Sort").withEstimatedRows(round((limit / math.sqrt(nodeCount)) * nodeCount))
            .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount))
          )
        )
      )
  }

  test("should estimate rows with respect to SKIP when limit is higher than total nodes") {
    val nodeCount = 100
    val limit = 100
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) WITH n SKIP 5 RETURN n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit - PlannerDefaults.DEFAULT_SKIP_ROW_COUNT)
      .withLHS(aPlan("Limit").withEstimatedRows(limit - PlannerDefaults.DEFAULT_SKIP_ROW_COUNT)
        .withLHS(aPlan("Skip").withEstimatedRows(limit - PlannerDefaults.DEFAULT_SKIP_ROW_COUNT)
          .withLHS(aPlan("AllNodesScan").withEstimatedRows(limit))
        )
      )
  }

  test("should still reflect SKIP after LIMIT") {
    val nodeCount = 10
    val limit = 5
    (0 until nodeCount).foreach(_ => createNode())

    val result = executeSingle(s"MATCH (n) WITH n SKIP 5 RETURN n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("Skip").withEstimatedRows(limit)
          .withLHS(aPlan("AllNodesScan").withEstimatedRows(limit + PlannerDefaults.DEFAULT_SKIP_ROW_COUNT)
          )
        )
      )
  }

  test("should reset selectivity through eager procedure") {
    val nodeCount = 1000
    val limit = 30
    (0 until nodeCount).foreach(_ => createNode())

    registerProcedure("my.first.proc") { builder =>
      builder.eager(true)
        .out(VOID)

      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyValue],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] =
          RawIterator.of[Array[AnyValue], ProcedureException](input)
      }
    }

    val result = executeSingle(s"profile MATCH (n) CALL my.first.proc() RETURN n LIMIT $limit")

    result.executionPlanDescription() should haveAsRoot.aPlan("ProduceResults").withEstimatedRows(limit)
      .withLHS(aPlan("Limit").withEstimatedRows(limit)
        .withLHS(aPlan("ProcedureCall").withEstimatedRows(limit)
          .withLHS(aPlan("Eager").withEstimatedRows(round(limit / PlannerDefaults.DEFAULT_MULTIPLIER.coefficient))
            .withLHS(aPlan("AllNodesScan").withEstimatedRows(nodeCount)
            )
          )
        )
      )
  }

  test("semiApply with increasing cardinality in rhs") {
    createNodesAndRels(nodeCount = 100, relCount = 1000)

    val result = executeSingle(s"CYPHER runtime=slotted MATCH (n) WHERE (n)-->() RETURN n")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("SemiApply")
      // -- per row: original=100, fraction=1/100, effective=100*1/100=1 -- reported: 1*100
      .withRHS(aPlan("Expand(All)").withEstimatedRows(100)
           // -- per row: original=1, fraction=1/100, effective=1*1/100=1/100 -rounded-> 1 -- reported: 1*100
          .withLHS(aPlan("Argument").withEstimatedRows(100)))
      // -- original: 100
      .withLHS(aPlan("AllNodesScan").withEstimatedRows(100))
  }

  test("antiSemiApply with increasing cardinality in rhs") {
    createNodesAndRels(nodeCount = 100, relCount = 1000)

    val result = executeSingle(s"CYPHER runtime=slotted MATCH (n) WHERE NOT (n)-->() RETURN n")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("AntiSemiApply")
      // -- per row: original=100, fraction=1/100, effective=100*1/100=1 -- reported: 1*100
      .withRHS(aPlan("Expand(All)").withEstimatedRows(100)
         // -- per row: original=1, fraction=1/100, effective=1*1/100=1/100 -rounded-> 1 -- reported: 1*100
        .withLHS(aPlan("Argument").withEstimatedRows(100)))
      // -- original: 100
      .withLHS(aPlan("AllNodesScan").withEstimatedRows(100))
  }

  test("semiApply with increasing cardinality in rhs under limit") {
    createNodesAndRels(100, 1000)
    val limit = 5

    val result = executeSingle(s"CYPHER runtime=slotted MATCH (n) WHERE (n)-->() RETURN n LIMIT $limit")

    result.executionPlanDescription() should includeSomewhere
       // original: 75, fraction: 5/75
      .aPlan("SemiApply").withEstimatedRows(5)
      // -- per row: original=100, fraction=5/75*1/7, effective=100*5/75*1/7~=1 -- reported: 1*7
      .withRHS(aPlan("Expand(All)").withEstimatedRows(7)
        // -- per row: original=1, fraction=5/75*1/7, effective=1*5/75*1/7 -rounded-> 1 -- reported: 1*7
        .withLHS(aPlan("Argument").withEstimatedRows(7)))
      // -- original: 100, fraction 5/75
      .withLHS(aPlan("AllNodesScan").withEstimatedRows(7))
  }

  test("semiApply with label scan and increasing cardinality in rhs") {
    // 10x :A
    // 12x :B
    // 1000x (:A)-->(:B)
    createAsAndBs(aNodes = 10, bNodes = 12, abRelsPerPair = 100)

    val result = executeSingle(s"CYPHER runtime=slotted MATCH (a:A) WHERE (a)-->(:B) RETURN a")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("SemiApply").withEstimatedRows(7)
      // -- per row: original=100, fraction=1/100, effective=100*1/100=1 -- reported: 10
      .withRHS(aPlan("Expand(Into)").withEstimatedRows(10)
        // -- per row: original=12, fraction=1/100, effective=12*1/100=0.12 -- reported: 1
        .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(1)))
      // -- original: 10
      .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(10))
  }

  test("semiApply with label scan and increasing then decreasing cardinality in rhs") {
    // 10x :A
    // 12x :B
    // 1000x (:A)-->(:B)
    createAsAndBs(aNodes = 10, bNodes = 12, abRelsPerPair = 100)

    val result = executeSingle(s"CYPHER runtime=slotted MATCH (a:A) WHERE (a)-[{x: 1}]->(:B) RETURN a")

    result.executionPlanDescription() should includeSomewhere
      .aPlan("SemiApply").withEstimatedRows(7)
      // -- per row: original=10, fraction=1/10=0.1, effective=10*0.1=1 -- reported: 1*10
      .withRHS(aPlan("Filter").withEstimatedRows(10)
        // -- per row: original=100, fraction=0.1, effective=100*0.1=10 -- reported: 10*10
        .withLHS(aPlan("Expand(Into)").withEstimatedRows(100)
          // -- per row: original=12, fraction=0.1, effective=12*0.1=1.2 -- reported: 1.2*10
          .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(12))))
      // -- original: 10
      .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(10))
  }

  test("semiApply with label scan and increasing then decreasing cardinality in rhs under limit") {
    // 10x :A
    // 12x :B
    // 1000x (:A)-->(:B)
    createAsAndBs(aNodes = 10, bNodes = 12, abRelsPerPair = 100)
    val result = executeSingle(s"CYPHER runtime=slotted MATCH (a:A) WHERE (a)-[{x: 1}]->(:B) RETURN a LIMIT 5")

    result.executionPlanDescription() should includeSomewhere
      // original: 7.5, fraction: 5/7.5
      .aPlan("SemiApply").withEstimatedRows(5)
      // -- per row: original=10, fraction=5/7.5*1/7.5, effective=10*5/7.5*1/7.5~=0.8 -- reported: round(0.8*7.5)=7
      .withRHS(aPlan("Filter").withEstimatedRows(7)
        // -- per row: original=100, fraction=5/7.5*1/7.5, effective=100*5/7.5*1/7.5~=8.8 -- reported: round(8.8*7.5)=67
        .withLHS(aPlan("Expand(Into)").withEstimatedRows(67)
          // -- per row: original=12, fraction=5/7.5*1/7.5, effective=12*5/7.5*1/7.5~=1.1 -- reported: round(1.1*7.5)=8
          .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(8))))
      // original: 10, fraction: 5/7.5
      .withLHS(aPlan("NodeByLabelScan").withEstimatedRows(7))
  }

  private def createNodesAndRels(nodeCount: Int, relCount: Int) = {
    createAsAndBs(nodeCount/2, nodeCount/2, relCount/(nodeCount/2))
    (nodeCount, relCount)
  }

  private def createAsAndBs(aNodes: Int, bNodes: Int, abRelsPerPair: Int): Unit = {
    val shared = Math.min(aNodes, bNodes)
    (0 until shared).foreach { _ =>
      val a = createLabeledNode("A")
      val b = createLabeledNode("B")
      (0 until abRelsPerPair).foreach(_ => relate(a, b))
    }
    (shared until aNodes).foreach(_ => createLabeledNode("A"))
    (shared until bNodes).foreach(_ => createLabeledNode("B"))
  }
}
