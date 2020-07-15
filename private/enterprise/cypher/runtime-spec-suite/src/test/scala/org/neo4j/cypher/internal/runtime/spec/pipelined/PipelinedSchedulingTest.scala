/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.pipelined

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.PipelinedRuntime.PIPELINED
import org.neo4j.cypher.internal.runtime.spec.ENTERPRISE
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.pipelined.PipelinedSchedulingTest.CollectScheduledWorkIds
import org.neo4j.cypher.internal.runtime.spec.pipelined.tracing.Event
import org.neo4j.cypher.internal.runtime.spec.pipelined.tracing.TASK_SCHEDULE
import org.neo4j.cypher.internal.runtime.spec.pipelined.tracing.TracerVerifier
import org.neo4j.cypher.internal.runtime.spec.pipelined.tracing.VerifyingSchedulerTracer
import org.neo4j.cypher.internal.util.attribution.Id

import scala.collection.mutable

class PipelinedSchedulingTest extends RuntimeTestSuite[EnterpriseRuntimeContext](
  ENTERPRISE.WITH_TRACER(new VerifyingSchedulerTracer(CollectScheduledWorkIds)),
  PIPELINED) {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    CollectScheduledWorkIds.clearEvents()
  }

  test("join on the rhs of apply should alternate LHS and RHS") {
    // given
    nodeGraph(3)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x") // 0
      .apply() // 1
      .|.valueHashJoin("x = x") // 2
      .|.|.argument("x") // 3
      .|.argument("x") // 4
      .allNodeScan("x") // 5
      .build()

    // when
    val result = execute(logicalQuery, runtime)
    consume(result)

    val ANS = Id(5)
    val LHS = Id(4)
    val RHS = Id(3)
    val JOIN = Id(2)

    // then
    val expectedEvents = Seq(
      ANS,
      LHS, // This will build 2 HashTables
      ANS, // We currently let ANS run until the ApplyBuffer `!canPut`. Ideally we would schedule RHS here.
      RHS, // After this we can probe these 2 HashTables
      JOIN,
      JOIN,
      LHS, // Build 1 more HashTable
      RHS,
      JOIN, // Probe the last HashTable
    )
    CollectScheduledWorkIds.getEvents should equal(expectedEvents)
  }
}

object PipelinedSchedulingTest {
  // DO NOT USE IN PARALLEL (i.e. not in multiple test classes)
  private[PipelinedSchedulingTest] object CollectScheduledWorkIds extends TracerVerifier {
    private val events = new mutable.ArrayBuffer[Id]()

    override def apply(e: Event): Unit = e match {
      case TASK_SCHEDULE(_, workId, _, _) => events += workId
      case _ =>
    }

    def getEvents: Seq[Id] = events

    def clearEvents(): Unit = events.clear()
  }
}
