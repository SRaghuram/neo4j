/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.VOID
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

abstract class ProfileTimeTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              val sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  // We always get OperatorProfile.NO_DATA for page cache hits and misses in Pipelined
  private val NO_PROFILE = new OperatorProfile.ConstOperatorProfile(0, 0, 0, OperatorProfile.NO_DATA, OperatorProfile.NO_DATA)

  // time is profiled in nano-seconds, but we can only assert > 0, because the operators take
  // different time on different tested systems.

  test("should profile time of all nodes scan + produce results") {
    given { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // all nodes scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with apply") {
    val size = sizeHint / 10
    given {
      nodePropertyGraph(size, {
        case i => Map("prop" -> i)
      })
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .filter(s"x.prop < ${size / 4}")
      .apply()
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // filter
    queryProfile.operatorProfile(2).time() should be > 0L // apply -  time of the output task of the previous pipeline gets attributed here
    queryProfile.operatorProfile(3).time() should be > 0L // filter
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    queryProfile.operatorProfile(5).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with hash join") {
    val size = sizeHint / 10
    given { nodeGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // hash join
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with expand all") {
    val size = sizeHint / 10
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // expand all
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with var expand") {
    val size = sizeHint / 10
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*1..5]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // var expand
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with pruning var expand") {
    val size = sizeHint / 10
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .pruningVarExpand("(x)-[*1..5]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // pruning var expand
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with shortest path") {
    // given
    val nodesPerLabel = 10
    given {
      for(_ <- 0 until nodesPerLabel)
        sineGraph()
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath("(x)-[r*]-(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END")
      .nodeByLabelScan("x", "START")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // shortest path
    queryProfile.operatorProfile(2).time() should be > 0L // cartesian product
    queryProfile.operatorProfile(3).time() should be > 0L // nodeByLabelScan
    queryProfile.operatorProfile(4).time() should be > 0L // nodeByLabelScan
  }

  test("should profile time with expand into") {
    val size = sizeHint / 10
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-[r]->(y)")
      .expand("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // expand into
    queryProfile.operatorProfile(2).time() should be > 0L // expand
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with optional expand all") {
    // given
    val size = sizeHint / 10
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional expand all
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with optional expand into") {
    // given
    val size = Math.sqrt(sizeHint).toInt
    given { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandInto("(x)-->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional expand into
    queryProfile.operatorProfile(2).time() should be > 0L // cartesian product
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time with optional") {
    val size = sizeHint / 10
    given { nodeGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional()
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time of sort") {
    given { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort(Seq(Ascending("x")))
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // sort
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }

  test("should profile time of cartesian product") {
    val size = Math.sqrt(sizeHint).toInt
    given { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).time() should be > 0L// cartesian product
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan b
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan a
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should be(NO_PROFILE)
  }
}

trait ProcedureCallTimeTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileTimeTestBase[CONTEXT] =>

  test("should profile time of procedure call") {
    // given
    registerProcedure(new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "proc").mode(Mode.READ).out(VOID).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        RawIterator.empty[Array[AnyValue], ProcedureException]()
      }
    })

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("j")
      .procedureCall("proc()")
      .unwind(s"range(0, ${sizeHint - 1}) AS j")
      .argument()
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    result.runtimeResult.queryProfile().operatorProfile(1).time() should be > 0L // procedure call
    result.runtimeResult.queryProfile().operatorProfile(2).time() should be > 0L // unwind
    result.runtimeResult.queryProfile().operatorProfile(3).time() should be > 0L // argument
  }
}
