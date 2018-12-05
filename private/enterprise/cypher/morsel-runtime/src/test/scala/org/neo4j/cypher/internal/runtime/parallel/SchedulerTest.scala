/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.parallel

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConversions._
import scala.collection.mutable

case object Resource extends AutoCloseable { override def close(): Unit = {} }

abstract class SchedulerTest extends CypherFunSuite {

  private val tracer = SchedulerTracer.NoSchedulerTracer

  def newScheduler(maxConcurrency: Int): Scheduler[Resource.type]

  test("execute simple task") {

    val s = newScheduler( 1 )

    val testThread = Thread.currentThread().getId
    val taskThreadId = new AtomicLong(testThread)

    val sb = new StringBuilder
    val queryExecution = s.execute(tracer, IndexedSeq(NoopTask(() => {
          sb ++= "great success"
          taskThreadId.set(Thread.currentThread().getId)
        })))

    queryExecution.await()

    sb.result() should equal("great success")
    if (s.isMultiThreaded)
      taskThreadId.get() should not equal testThread
  }

  test("execute 1000 simple tasks, spread over 4 threads") {
    val concurrency = 4
    val s = newScheduler(concurrency)

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( i <- 0 until 1000 ) yield
        s.execute(tracer, IndexedSeq(NoopTask(() => {
                  map.put(i, Thread.currentThread().getId)
                })))

    futures.foreach(f => f.await())

    if (s.isMultiThreaded) {
      val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
      countsPerThread.size() should equal(concurrency)
    }
  }

  test("execute downstream tasks") {

    val s = newScheduler(2)

    val result: mutable.Set[String] = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

    val queryExecution = s.execute(tracer, IndexedSeq(SubTasker(List(
            NoopTask(() => result += "once"),
            NoopTask(() => result += "upon"),
            NoopTask(() => result += "a"),
            NoopTask(() => result += "time")
          ))))

    queryExecution.await()
    result should equal(Set("once", "upon", "a", "time"))
  }

  test("execute reduce-like task tree") {

    val s = newScheduler(64)

    val aggregator = SumAggregator()

    val tasks = SubTasker(List(
      PushToEager(List(1,10,100), aggregator),
      PushToEager(List(1000,10000,100000), aggregator)))

    val queryExecution = s.execute(tracer, IndexedSeq(tasks))

    queryExecution.await()
    aggregator.sum.get() should be(111111)
  }

  test("should execute multiple initial tasks") {
    val concurrency = 4
    val s = newScheduler(concurrency)

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( slice <- (0 until 1000).grouped(9) ) yield {
        val tasks: IndexedSeq[NoopTask] = slice.map(i => NoopTask(() => {
          map.put(i, Thread.currentThread().getId)
        }))

        s.execute(tracer, tasks)
      }

    futures.foreach(f => f.await())

    if (s.isMultiThreaded) {
      val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
      countsPerThread.size() should equal(concurrency)
    }
  }

  // HELPER TASKS

  case class SumAggregator() extends Task[Resource.type] {

    val buffer = new ConcurrentLinkedQueue[Integer]
    val sum = new AtomicInteger()

    override def executeWorkUnit(resource: Resource.type): Seq[Task[Resource.type]] = {
      var value = buffer.poll()
      while (value != null) {
        sum.addAndGet(value)
        value = buffer.poll()
      }
      Nil
    }

    override def canContinue: Boolean = buffer.nonEmpty

    override def workId: Int = 0

    override def workDescription: String = getClass.getSimpleName
  }

  case class PushToEager(subResults: Seq[Int], eager: SumAggregator) extends Task[Resource.type] {

    private val resultSequence = subResults.iterator

    override def executeWorkUnit(resource: Resource.type): Seq[Task[Resource.type]] = {
      if (resultSequence.hasNext)
        eager.buffer.add(resultSequence.next())

      if (canContinue) Nil
      else List(eager)
    }

    override def canContinue: Boolean = resultSequence.hasNext

    override def workId: Int = 1

    override def workDescription: String = getClass.getSimpleName
  }

  case class SubTasker(subtasks: Seq[Task[Resource.type]]) extends Task[Resource.type] {

    private val taskSequence = subtasks.iterator

    override def executeWorkUnit(resource: Resource.type): Seq[Task[Resource.type]] =
      if (taskSequence.hasNext) List(taskSequence.next())
      else Nil

    override def canContinue: Boolean = taskSequence.nonEmpty

    override def workId: Int = 2

    override def workDescription: String = getClass.getSimpleName
  }

  case class NoopTask(f: () => Any) extends Task[Resource.type] {
    override def executeWorkUnit(resource: Resource.type): Seq[Task[Resource.type]] = {
      f()
      Nil
    }

    override def canContinue: Boolean = false

    override def workId: Int = 3

    override def workDescription: String = getClass.getSimpleName
  }
}
