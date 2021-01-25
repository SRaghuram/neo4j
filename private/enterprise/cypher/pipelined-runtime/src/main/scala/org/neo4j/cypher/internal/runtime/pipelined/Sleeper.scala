/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper.ACTIVE
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper.SLEEPING
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper.Status
import org.neo4j.cypher.internal.runtime.pipelined.Sleeper.WORKING

import scala.concurrent.duration.Duration

sealed trait Sleeper {
  def reportStartWorkUnit(): Unit
  def reportStopWorkUnit(): Unit
  def reportIdle(): Unit
  def isSleeping: Boolean
  def isWorking: Boolean
  def statusString: String
}

object Sleeper {

  sealed trait Status

  case object ACTIVE extends Status // while the worker is not doing any of the below, e.g. looking for work
  case object WORKING extends Status // while the worker is executing a work unit of some query
  case object SLEEPING extends Status // while the worker is parked and not looking for work

  case object noSleep extends Sleeper {
    override def reportStartWorkUnit(): Unit = {}
    override def reportStopWorkUnit(): Unit = {}
    override def reportIdle(): Unit = {}
    override def isSleeping: Boolean = false
    //We must report false otherwise the leak detection thinks we are leaking resources
    override def isWorking: Boolean = false
    override def statusString: String = Sleeper.ACTIVE.toString
  }

  def concurrentSleeper(workerId: Int, sleepDuration: Duration = Duration(1, TimeUnit.SECONDS)): Sleeper =
    new ConcurrentSleeper(workerId, sleepDuration)
}

class ConcurrentSleeper(val workerId: Int,
                        private val sleepDuration: Duration = Duration(1, TimeUnit.SECONDS)) extends Sleeper {

  private val sleepNs = sleepDuration.toNanos
  private var workStreak = 0
  @volatile private var status: Status = ACTIVE

  override def reportStartWorkUnit(): Unit = {
    status = WORKING
  }

  override def reportStopWorkUnit(): Unit = {
    status = ACTIVE
    workStreak += 1
  }

  override def reportIdle(): Unit = {
    DebugSupport.WORKERS.log("[WORKER%2d] parked after working %d times", workerId, workStreak)
    workStreak = 0
    status = SLEEPING
    LockSupport.parkNanos(sleepNs)
    DebugSupport.WORKERS.log("[WORKER%2d] unparked", workerId)
    status = ACTIVE
  }

  override def isSleeping: Boolean = status == SLEEPING

  override def isWorking: Boolean = status == WORKING

  override def statusString: String = status.toString
}
