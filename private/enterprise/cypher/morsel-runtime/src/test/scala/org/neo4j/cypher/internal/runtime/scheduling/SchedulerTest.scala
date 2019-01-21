/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.scheduling

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

import org.mockito.Mockito
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConversions._
import scala.collection.mutable

case object Resource extends AutoCloseable { override def close(): Unit = {} }

abstract class SchedulerTest extends CypherFunSuite {

  private val tracer = SchedulerTracer.NoSchedulerTracer

  def newScheduler(maxConcurrency: Int): Scheduler[Resource.type]

  def shutDown(): Unit

  override protected def stopTest(): Unit = shutDown()

  test("execute simple task") {
    val s = newScheduler( 1 )

    val testThread = Thread.currentThread().getId
    val taskThreadId = new AtomicLong(testThread)

    val sb = new StringBuilder
    val queryExecution = s.execute(tracer, IndexedSeq(NoopTask(() => {
          sb ++= "great success"
          taskThreadId.set(Thread.currentThread().getId)
        })))

    val result = queryExecution.await()
    result shouldBe empty // no exception

    sb.result() should equal("great success")
    if (s.isMultiThreaded)
      taskThreadId.get() should not equal testThread
  }

  test("should trace single query single task") {
    val verifier = mock[TracerVerifier]

    val s = newScheduler( 4 )

    val task = NoopTask(() => {})
    val queryExecution = s.execute(new VerifyingSchedulerTracer(verifier), IndexedSeq(task))

    val result = queryExecution.await()
    result shouldBe empty // no exception

    val order = Mockito.inOrder(verifier)
    order.verify(verifier)(QUERY_START(0))
    order.verify(verifier)(TASK_SCHEDULE(0, task, 0, None))
    order.verify(verifier)(TASK_START(0, task, 0))
    order.verify(verifier)(TASK_STOP(0, task, 0))
    order.verify(verifier)(QUERY_STOP(0))
    order.verifyNoMoreInteractions()
  }

  test("should trace multiple queries") {
    val verifier = mock[TracerVerifier]

    val s = newScheduler( 4 )

    val task1 = NoopTask(() => {})
    val task2 = NoopTask(() => {})
    val tracer = new VerifyingSchedulerTracer(verifier)
    val queryExecution1 = s.execute(tracer, IndexedSeq(task1))
    val queryExecution2 = s.execute(tracer, IndexedSeq(task2))

    queryExecution1.await() shouldBe empty // no exception
    queryExecution2.await() shouldBe empty // no exception

    val order1 = Mockito.inOrder(verifier)
    order1.verify(verifier)(QUERY_START(0))
    order1.verify(verifier)(TASK_SCHEDULE(0, task1, 0, None))
    order1.verify(verifier)(TASK_START(0, task1, 0))
    order1.verify(verifier)(TASK_STOP(0, task1, 0))
    order1.verify(verifier)(QUERY_STOP(0))
    val order2 = Mockito.inOrder(verifier)
    order2.verify(verifier)(QUERY_START(1))
    order2.verify(verifier)(TASK_SCHEDULE(1, task2, 0, None))
    order2.verify(verifier)(TASK_START(1, task2, 0))
    order2.verify(verifier)(TASK_STOP(1, task2, 0))
    order2.verify(verifier)(QUERY_STOP(1))
  }


  test("should trace single query task with continuation") {
    val verifier = mock[TracerVerifier]

    val s = newScheduler( 4 )

    val task = ContinueTask(3)
    val queryExecution = s.execute(new VerifyingSchedulerTracer(verifier), IndexedSeq(task))

    val result = queryExecution.await()
    result shouldBe empty // no exception

    val order = Mockito.inOrder(verifier)
    order.verify(verifier)(QUERY_START(0))
    order.verify(verifier)(TASK_SCHEDULE(0, task, 0, None))
    order.verify(verifier)(TASK_START(0, task, 0))
    order.verify(verifier)(TASK_STOP(0, task, 0))
    order.verify(verifier)(TASK_SCHEDULE(0, task, 1, Some(0)))
    order.verify(verifier)(TASK_START(0, task, 1))
    order.verify(verifier)(TASK_STOP(0, task, 1))
    order.verify(verifier)(TASK_SCHEDULE(0, task, 2, Some(1)))
    order.verify(verifier)(TASK_START(0, task, 2))
    order.verify(verifier)(TASK_STOP(0, task, 2))
    order.verify(verifier)(QUERY_STOP(0))
    order.verifyNoMoreInteractions()
  }

  test("should trace single query task with downstream tasks") {
    val verifier = mock[TracerVerifier]

    val s = newScheduler( 4 )

    val task3 = NoopTask(() => {})
    val task2 = SubTasker(Seq(task3))
    val task1 = SubTasker(Seq(task2))
    val queryExecution = s.execute(new VerifyingSchedulerTracer(verifier), IndexedSeq(task1))

    val result = queryExecution.await()
    result shouldBe empty // no exception

    val order = Mockito.inOrder(verifier)
    order.verify(verifier)(QUERY_START(0))
    order.verify(verifier)(TASK_SCHEDULE(0, task1, 0, None))
    order.verify(verifier)(TASK_START(0, task1, 0))
    order.verify(verifier)(TASK_STOP(0, task1, 0))
    order.verify(verifier)(TASK_SCHEDULE(0, task2, 1, Some(0)))
    order.verify(verifier)(TASK_START(0, task2, 1))
    order.verify(verifier)(TASK_STOP(0, task2, 1))
    order.verify(verifier)(TASK_SCHEDULE(0, task3, 2, Some(1)))
    order.verify(verifier)(TASK_START(0, task3, 2))
    order.verify(verifier)(TASK_STOP(0, task3, 2))
    order.verify(verifier)(QUERY_STOP(0))
    order.verifyNoMoreInteractions()
  }

  test("should trace single query with exception") {
    val verifier = mock[TracerVerifier]

    val s = newScheduler(4)

    // This task will on the first invocation fail and on the second do the provided function.
    // The second should never happen.
    val fail = FailTask(() => {})
    val sub = SubTasker(List(fail))
    val queryExecution = s.execute(new VerifyingSchedulerTracer(verifier), IndexedSeq(sub))

    val result = queryExecution.await()
    result shouldBe defined // we got an exception

    val order = Mockito.inOrder(verifier)
    order.verify(verifier)(QUERY_START(0))
    order.verify(verifier)(TASK_SCHEDULE(0, sub, 0, None))
    order.verify(verifier)(TASK_START(0, sub, 0))
    order.verify(verifier)(TASK_STOP(0, sub, 0))

    order.verify(verifier)(TASK_SCHEDULE(0, fail, 1, Some(0)))
    order.verify(verifier)(TASK_START(0, fail, 1))
    order.verify(verifier)(TASK_STOP(0, fail, 1))

    order.verify(verifier)(QUERY_STOP(0))
    order.verifyNoMoreInteractions()
  }

  test("execute 1000 simple tasks, spread over 4 threads") {
    val concurrency = 4
    val s = newScheduler(concurrency)

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( i <- 0 until 1000 ) yield
        s.execute(tracer, IndexedSeq(NoopTask(() => {
          map.put(i, Thread.currentThread().getId)
          // If we make the tasks too fast, it might be that one worker doesn't get any share of the work
          Thread.sleep(1)
        })))

    futures.foreach(f => f.await() shouldBe empty)

    if (s.isMultiThreaded) {
      val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
      countsPerThread.size() should equal(concurrency)
    }
  }

  test("execute downstream tasks") {

    val s = newScheduler(2)

    val mutableSet: mutable.Set[String] = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

    val queryExecution = s.execute(tracer, IndexedSeq(SubTasker(List(
            NoopTask(() => mutableSet += "once"),
            NoopTask(() => mutableSet += "upon"),
            NoopTask(() => mutableSet += "a"),
            NoopTask(() => mutableSet += "time")
      ))))

    val result = queryExecution.await()
    result shouldBe empty // no exception
    mutableSet should equal(Set("once", "upon", "a", "time"))
  }

  test("abort in flight tasks on failure") {

    val s = newScheduler(2)

    val mutableSet: mutable.Set[String] = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

    val queryExecution = s.execute(tracer,
      IndexedSeq(SubTasker(List(
        NoopTask(() => mutableSet += "a"),
        // This task will on the first invocation fail and on the second do the provided function.
        // The second should never happen.
        FailTask(() => mutableSet += "b"),
        NoopTask(() => mutableSet += "c") // Depending on the scheduler, this task might execute
      ))))

    val result = queryExecution.await()
    result shouldBe defined // we got an exception
    mutableSet should(equal(Set("a")) or equal(Set("a", "c")))
  }

  test("abort should not affect other query executions") {

    val s = newScheduler(2)

    val mutableSet: mutable.Set[String] = java.util.concurrent.ConcurrentHashMap.newKeySet[String]()

    val queryExecution1 = s.execute(tracer,
      IndexedSeq(SubTasker(List(
        NoopTask(() => mutableSet += "a"),
        // This task will on the first invocation fail and on the second do the provided function.
        // The second should never happen.
        FailTask(() => mutableSet += "b"),
        NoopTask(() => mutableSet += "c") // Depending on the scheduler, this task might execute
          ))))

    val queryExecution2 = s.execute(tracer,
      IndexedSeq(SubTasker(List(
        NoopTask(() => mutableSet += "once"),
        NoopTask(() => mutableSet += "upon"),
        NoopTask(() => mutableSet += "time")
      ))))

    val result1 = queryExecution1.await()
    val result2 = queryExecution2.await()
    result1 shouldBe defined // we got an exception
    result2 shouldBe empty // no exception
    mutableSet should contain allOf ("once", "upon", "a", "time")
  }

  test("execute reduce-like task tree") {

    val s = newScheduler(64)

    val aggregator = SumAggregator()

    val tasks = SubTasker(List(
      PushToEager(List(1,10,100), aggregator),
      PushToEager(List(1000,10000,100000), aggregator)))

    val queryExecution = s.execute(tracer, IndexedSeq(tasks))

    val result = queryExecution.await()
    result shouldBe empty // no exception

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

  case class FailTask(f: () => Any) extends Task[Resource.type ] {

    @volatile
    private var failedAlready = false

    override def executeWorkUnit(threadLocalResource: Resource.type): Seq[Task[Resource.type]] = if (failedAlready) {
      f()
      Nil
    } else {
      failedAlready = true
      throw new Exception("Task failed")
    }

    override def canContinue: Boolean = true

    override def workId: Int = 4

    override def workDescription: String = getClass.getSimpleName
  }

  case class ContinueTask(private var times: Int) extends Task[Resource.type] {
    override def executeWorkUnit(resource: Resource.type): Seq[Task[Resource.type]] = {
      times -= 1
      Nil
    }

    override def canContinue: Boolean = times > 0

    override def workId: Int = 5

    override def workDescription: String = getClass.getSimpleName
  }
}
