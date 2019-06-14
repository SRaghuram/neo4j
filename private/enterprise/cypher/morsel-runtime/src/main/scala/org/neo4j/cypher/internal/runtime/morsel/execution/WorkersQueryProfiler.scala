/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.execution

import org.neo4j.cypher.internal.profiling.{NoKernelStatisticProvider, ProfilingTracer, ProfilingTracerData, QueryProfiler}
import org.neo4j.cypher.result.{OperatorProfile, QueryProfile}

/**
  * Keeps track of one [[QueryProfiler]] per worker, which means that we do not need
  * any synchronization during query execution.
  */
trait WorkersQueryProfiler extends QueryProfile {
  def queryProfiler(workerId: Int): QueryProfiler
}

object WorkersQueryProfiler {
  val NONE: WorkersQueryProfiler = new WorkersQueryProfiler {
    override def queryProfiler(workerId: Int): QueryProfiler = QueryProfiler.NONE
    override def operatorProfile(operatorId: Int): OperatorProfile = OperatorProfile.NONE
  }
}

class FixedWorkersQueryProfiler(numberOfWorkers: Int) extends WorkersQueryProfiler {

  private val profilers: Array[ProfilingTracer] =
    (0 until numberOfWorkers).map(_ => new ProfilingTracer(NoKernelStatisticProvider)).toArray

  override def queryProfiler(workerId: Int): QueryProfiler = {
    profilers(workerId)
  }

  override def operatorProfile(operatorId: Int): OperatorProfile = {
    var i = 0
    val data = new ProfilingTracerData()
    while (i < numberOfWorkers) {
      val workerData = profilers(i).operatorProfile(operatorId)
      data.update(workerData.time(),
                  workerData.dbHits(),
                  workerData.rows(),
                  0,
                  0)

      i += 1
    }
    data.update(0, 0, 0, OperatorProfile.NO_DATA, OperatorProfile.NO_DATA)
    data
  }
}
