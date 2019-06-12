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
trait WorkersQueryProfiler {
  def queryProfiler(workerId: Int): QueryProfiler
}

object WorkersQueryProfiler {
  val NONE: WorkersQueryProfiler = (workerId: Int) => QueryProfiler.NONE
}

/**
  * @param numberOfWorkers number of worker that can execute this query
  * @param applyRhsPlans maps all apply ids to their corresponding rhs operator id. This is needed because the apply
  *                      operator is not an executable operator in morsel, and thus there is no place to inject code
  *                      for counting apply rows. Instead we return the rhs operator rows, as these are guaranteed to
  *                      be identical.
  */
class FixedWorkersQueryProfiler(numberOfWorkers: Int, applyRhsPlans: Map[Int, Int]) extends WorkersQueryProfiler {

  private val profilers: Array[ProfilingTracer] =
    (0 until numberOfWorkers).map(_ => new ProfilingTracer(NoKernelStatisticProvider)).toArray

  override def queryProfiler(workerId: Int): QueryProfiler = {
    profilers(workerId)
  }

  /**
    * The Profile that this profiler creates.
    */
  object Profile extends QueryProfile {
    override def operatorProfile(operatorId: Int): OperatorProfile = {
      applyRhsPlans.get(operatorId) match {
        case Some(applyRhsPlanId) => applyOperatorProfile(applyRhsPlanId)
        case None => regularOperatorProfile(operatorId)
      }
    }

    private def regularOperatorProfile(operatorId: Int): OperatorProfile = {
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

    private def applyOperatorProfile(applyRhsPlanId: Int): OperatorProfile = {
      var i = 0
      val data = new ProfilingTracerData()
      while (i < numberOfWorkers) {
        val workerData = profilers(i).operatorProfile(applyRhsPlanId)
        data.update(0,
          0,
          workerData.rows(),
          0,
          0)

        i += 1
      }
      data.update(0, 0, 0, OperatorProfile.NO_DATA, OperatorProfile.NO_DATA)
      data
    }
  }
}
