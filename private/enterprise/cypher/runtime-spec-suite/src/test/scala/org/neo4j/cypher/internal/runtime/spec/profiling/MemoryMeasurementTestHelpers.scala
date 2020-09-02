/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.profiling

import java.nio.file.Path

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.HighWaterScopedMemoryTracker
import org.neo4j.cypher.internal.runtime.InputCursor
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.MemoryTrackingController.MemoryTrackerDecorator
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.NonRecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.tests.InputStreams
import org.neo4j.cypher.internal.runtime.spec.util.HeapDumpReader
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.memory.HeapDumper
import org.neo4j.memory.HeapDumpingMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.test.rule.TestDirectory
import org.neo4j.values.AnyValue
import org.scalactic.TripleEqualsSupport.Spread
import org.scalatest.Args
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Status
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.util.Random

trait InputStreamHelpers[CONTEXT <: RuntimeContext] extends InputStreams[CONTEXT] {
  self: RuntimeTestSuite[CONTEXT] =>

  protected def finiteSeqInput(seq: Seq[Array[Any]]): InputDataStream =
    finiteGeneratedInput(seq.size)(i => seq(i.toInt - 1))

  protected def finiteSeqColumnInput(seq: Seq[Any]): InputDataStream =
    finiteGeneratedInput(seq.size)(i => Array(seq(i.toInt - 1)))

  protected def finiteConstantInput(length: Int)(row: Array[Any]): InputDataStream =
    finiteGeneratedInput(length)(_ => row)

  protected def finiteGeneratedInput(length: Int)(rowGen: Long => Array[Any]): InputDataStream =
    finiteInput(length, Some(rowGen))

  protected def withRandom[T](func: Random => T): T =
    func(new Random(seed = 1337))

  protected def withShuffle[T](size: Int)(func: (Long => Int) => T): T =
    withRandom { random =>
      val ids = Range(0, size).toIndexedSeq
      val shuffle = random.shuffle(ids)
      func(l => shuffle(l.toInt - 1))
    }
}

trait MemoryMeasurementTestHelpers[CONTEXT <: RuntimeContext] extends BeforeAndAfterAll with TestName {
  this: RuntimeTestSuite[CONTEXT] with InputStreams[CONTEXT] =>

  protected def setMemoryTrackingDecorator(decorator: MemoryTrackerDecorator): Unit
  protected def resetMemoryTrackingDecorator(): Unit
  protected def debugPrint: Boolean
  protected def summaryPrint: Boolean
  protected def deleteHeapDumps: Boolean

  private val testDir = TestDirectory.testDirectory(getClass)
  testDir.prepareDirectory(getClass, null)

  private def debug(msg: => String): Unit = if (debugPrint)
    println(msg)

  private val measurementRecords =
    collection.mutable.ArrayBuffer.empty[Record]

  override protected def afterAll(): Unit = {
    if (summaryPrint)
      (Seq(Record.header) ++ measurementRecords.map(_.fields))
        .map(row => row.mkString(";"))
        .foreach(println)
    if (deleteHeapDumps)
      testDir.complete(true)
  }

  abstract override def run(testName: Option[String], args: Args): Status =
    super.run(testName, args)

  /**
   * Validate query estimated max allocated memory against a (hopefully) more accurate memory measurement
   *
   * @param baselineQuery A (simple) query to run to get a baseline memory consumption (of the dbms etc.)
   * @param measuredQuery The query to validate on
   * @param input Query input - used for (possibly multiple) baseline runs and measurement runs
   * @param measuringStrategy Strategy to get the (hopefully) more accurate memory measurement
   * @param baselineStrategy Strategy to use for the baseline measurement
   * @param tolerance Defines what counts as an acceptable difference between estimate and measurement
   */
  def validateMaxAllocatedMemoryEstimate(
    baselineQuery: LogicalQuery,
    measuredQuery: LogicalQuery,
    input: => InputDataStream,
    measuringStrategy: MeasuringStrategy,
    baselineStrategy: Option[MeasuringStrategy] = None,
    tolerance: Tolerance,
  ): Assertion = {
    val record = try {
      debug("baseline")
      val (_, baseline) = run(baselineQuery, input, baselineStrategy.getOrElse(measuringStrategy))

      debug("measurement")
      val (estimate, measurement) = run(measuredQuery, input, measuringStrategy)

      // Assume that getting measurement < baseline is just noise, and cap it to 0
      val consumption = Math.max(0L, measurement - baseline)
      val (error, fraction) = ErrorFractionTolerance.error(estimate, consumption)
      debug(s"consumption: $consumption ($measurement - $baseline = ${measurement - baseline})")
      debug(s"estimate: $estimate")

      MeasurementRecord(testName, estimate, consumption, error, fraction)

    } catch {
      case err: Throwable =>
        ErrorRecord(testName, err)
    }

    // For afterAll summary
    measurementRecords.append(record)

    record match {
      case r: MeasurementRecord => r.estimate should tolerance.matcher(r.measurement)
      case r: ErrorRecord       => throw r.error
    }
  }

  def run(query: LogicalQuery, input: => InputDataStream, strategy: MeasuringStrategy): (Long, Long) = {
    var execution: HeapMeasuringExecution = null
    try {
      execution = strategy.runMeasurement(query, input)
      val estimate = execution.runtimeResult.queryProfile().maxAllocatedMemory()
      val measurement = execution.measurements.max
      (estimate, measurement)
    } finally {
      if (deleteHeapDumps && execution != null) execution.cleanUp()
    }
  }

  /**
   * Creates a query of the form
   * INPUT x, y, z, ...
   * RETURN x, y, z, ...
   */
  def passThoughQuery(
    nodes: Seq[String] = Seq.empty,
    relationships: Seq[String] = Seq.empty,
    variables: Seq[String] = Seq.empty,
    nullable: Boolean = true): LogicalQuery =
    new LogicalQueryBuilder(this)
      .produceResults(nodes ++ relationships ++ variables: _*)
      .input(nodes, relationships, variables, nullable)
      .build()


  // --- Tolerance ------------------------------------------------------------

  sealed trait Tolerance {
    def matcher(measured: Long): Matcher[Long]
  }

  /** Tolerate errors up to a certain magnitude */
  case class AbsoluteErrorTolerance(tolerance: Long, unit: ByteUnit) extends Tolerance {
    require(tolerance > 0)

    def matcher(measured: Long): Matcher[Long] = (estimate: Long) => MatchResult(
      Spread(measured, unit.toBytes(tolerance)).isWithin(estimate),
      s"Estimate: $estimate was not within tolerance: $tolerance$unit of measured: $measured",
      s"Estimate: $estimate was within tolerance: $tolerance$unit of measured: $measured",
    )
  }

  /** Tolerate errors up to a certain fraction of measured consumption */
  case class ErrorFractionTolerance(tolerance: Double) extends Tolerance {
    require(tolerance > 0.0)

    def matcher(measured: Long): Matcher[Long] = (estimate: Long) => {
      val (error, errorFraction) = ErrorFractionTolerance.error(estimate, measured)
      debug(s"errorFraction: $errorFraction")
      MatchResult(
        errorFraction <= tolerance,
        s"Error fraction: $errorFraction was not within tolerance: $tolerance (error: $error, estimate: $estimate, measured: $measured)",
        s"Error fraction: $errorFraction was within tolerance: $tolerance (error: $error, estimate: $estimate, measured: $measured)",
      )
    }
  }

  object ErrorFractionTolerance {
    def error(estimate: Long, measured: Long): (Long, Double) = {
      val error = Math.abs(measured - estimate)
      val errorFraction = error.toDouble / measured.toDouble
      (error, errorFraction)
    }
  }

  /** Tolerate iff tolerated by at least one  */
  case class EitherTolerance(tolerances: Tolerance*) extends Tolerance {
    override def matcher(measured: Long): Matcher[Long] =
      tolerances.map(_.matcher(measured)).reduce(_ or _)
  }


  // --- Measuring strategies -------------------------------------------------

  sealed trait MeasuringStrategy {
    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution
  }

  /**
   * Runs query once and records estimate high water mark
   * Runs query again, triggering a heap dump when that same estimate high water mark is reached
   */
  case object HeapDumpAtEstimateHighWaterMark extends MeasuringStrategy {
    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val highWaterMark = runAndGetTriggerEstimate(query, input)
      debug(s"highWaterMark: $highWaterMark")
//      val trackerDecorator = HighWaterMarkHeapDumpTrackerDecorator(highWaterMark)
      val trackerDecorator = HighWaterMarkReleaseHeapDumpTrackerDecorator(highWaterMark)
      setMemoryTrackingDecorator(trackerDecorator)
      val measurementResult = profileNonRecording(query, runtime, input)
      val execution = new HeapDumpingExecution(measurementResult.runtimeResult, measurementResult.nonRecordingQuerySubscriber, heapDumpRecorder = trackerDecorator.recorder)
      execution.consumeAll()
      resetMemoryTrackingDecorator()
      execution
    }

    private def runAndGetTriggerEstimate(query: LogicalQuery, input: => InputDataStream): Long = {
      val result = profileNonRecording(query, runtime, input)
      consumeNonRecording(result)
      result.runtimeResult.queryProfile().maxAllocatedMemory()
    }
  }

  /**
   * Runs query once and records the input offset when the estimate reaches high water mark
   * Runs query again, triggering heap dumps when at, and around, that input offset (-1, 0, +1)
   */
  case object HeapDumpAtEstimateHighWaterMarkInputOffset extends MeasuringStrategy {

    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val triggerOffset = runAndGetTriggerOffset(query, input)
      debug(s"triggerOffset: $triggerOffset")
      val dumpingInput = HeapDumpAtOffsetInputDataStream(input, Set(triggerOffset))
      val result = runtimeTestSupport.profileNonRecording(query, runtime, dumpingInput)
      val execution = new HeapDumpingExecution(result.runtimeResult, result.nonRecordingQuerySubscriber, heapDumpRecorder = dumpingInput)
      execution.consumeAll()
      execution
    }

    private def runAndGetTriggerOffset(query: LogicalQuery, input: => InputDataStream): Long = {
      var result: NonRecordingRuntimeResult = null
      var highWaterMarkOffset = 0L
      var highWaterMarkMemory = 0L
      val offsetTrackingInput: OnNextInputDataStream = new OnNextInputDataStream(input) {
        override protected def onNext(offset: Long): Unit = {
          val currentMax = result.runtimeResult.queryProfile().maxAllocatedMemory()
          if (currentMax > highWaterMarkMemory) {
            highWaterMarkMemory = currentMax
            highWaterMarkOffset = offset
          }
        }
      }
      result = profileNonRecording(query, runtime, offsetTrackingInput)
      consumeNonRecording(result)
      highWaterMarkOffset
    }
  }

  /**
   * Runs query once and records the input offset when the estimate reaches high water mark
   * Runs query again, triggering heap dumps when at, and around, that input offset (-1, 0, +1)
   */
  case object HeapDumpAtEstimateHighWaterMarkInputOffset2 extends MeasuringStrategy {

    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val triggerOffset = runAndGetTriggerOffset(query, input)
      debug(s"triggerOffset: $triggerOffset")
      val dumpingInput = HeapDumpAtOffsetInputDataStream(input, Set(triggerOffset))
      val result = runtimeTestSupport.profileNonRecording(query, runtime, dumpingInput)
      val execution = new HeapDumpingExecution(result.runtimeResult, result.nonRecordingQuerySubscriber, heapDumpRecorder = dumpingInput)
      execution.consumeAll()
      execution
    }

    private def runAndGetTriggerOffset(query: LogicalQuery, input: => InputDataStream): Long = {
      val offsetTrackingInput = OffsetTrackingInputDataStream(input)
      var highWaterMarkOffset = 0L
      setMemoryTrackingDecorator(inner => new OnHighWaterMarkTracker(inner) {
        override def onHighWaterMark(highWaterMark: Long): Unit =
          highWaterMarkOffset = offsetTrackingInput.currentOffset
      })
      val result = profileNonRecording(query, runtime, offsetTrackingInput)
      consumeNonRecording(result)
      highWaterMarkOffset
    }
  }

  /**
   * Triggers heap dumps at certain intervals as the input is consumed
   */
  case class HeapDumpAtInputIntervals(interval: Long) extends MeasuringStrategy {
    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val dumpingInput = HeapDumpAtIntervalsInputDataStream(input, interval)
      val result = runtimeTestSupport.profileNonRecording(query, runtime, dumpingInput)
      val execution = new HeapDumpingExecution(result.runtimeResult, result.nonRecordingQuerySubscriber, heapDumpRecorder = dumpingInput)
      execution.consumeAll()
      execution
    }
  }

  /**
   * Triggers heap dumps at a certain offset as the input is consumed
   */
  case class HeapDumpAtInputOffset(offset: Long) extends MeasuringStrategy {
    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val dumpingInput = HeapDumpAtOffsetInputDataStream(input, Set(offset))
      val result = runtimeTestSupport.profileNonRecording(query, runtime, dumpingInput)
      val execution = new HeapDumpingExecution(result.runtimeResult, result.nonRecordingQuerySubscriber, heapDumpRecorder = dumpingInput)
      execution.consumeAll()
      execution
    }
  }

  /**
   * Triggers heap dumps at certain intervals as output is produced
   */
  case class HeapDumpAtOutputIntervals(interval: Long) extends MeasuringStrategy {
    def runMeasurement(query: LogicalQuery, input: => InputDataStream): HeapMeasuringExecution = {
      val dumpingSubscriber = HeapDumpAtIntervalsQuerySubscriber(new NonRecordingQuerySubscriber, interval)
      val result = runtimeTestSupport.profileWithSubscriber(query, runtime, dumpingSubscriber, input)
      val execution = new HeapDumpingExecution(result, dumpingSubscriber, heapDumpRecorder = dumpingSubscriber)
      execution.consumeAll()
      execution
    }
  }

  trait HeapMeasuringExecution {
    def measurements: List[Long]
    def runtimeResult: RuntimeResult
    def cleanUp(): Unit
  }


  // --- Heap dumping measuring -----------------------------------------------

  class HeapDumpingExecution(
    val runtimeResult: RuntimeResult,
    nonRecordingQuerySubscriber: QuerySubscriber,
    heapDumpRecorder: => HeapDumpRecorder) extends HeapMeasuringExecution {

    def consumeAll(): Unit = {
      runtimeResult.consumeAll()
      runtimeResult.close()
    }

    override def measurements: List[Long] = {
      val threadName = Thread.currentThread().getName
      heapDumpRecorder.heapDumps.map { path =>
        val size = HeapDumpReader.load(path).threadReachableHeap(threadName)
        debug(s"$path: $size")
        size
      }
    }

    def cleanUp(): Unit = heapDumpRecorder.cleanUp()
  }

  trait HeapDumpRecorder {
    private val heapDumper = new HeapDumper
    private val random = new Random()
    private var _heapDumps = List.empty[Path]
    def heapDumps: List[Path] = _heapDumps

    protected def recordDump(path: Path): Unit =
      _heapDumps = _heapDumps :+ path

    protected def filePath(name: String): Path = {
      val id = name + "-" + random.alphanumeric.take(10).mkString
      testDir.filePath(id + ".hprof")
    }

    protected def takeDump(name: String): Path = {
      val path = filePath(name)
      heapDumper.createHeapDump(path.toAbsolutePath.toString, true)
      recordDump(path)
      path
    }

    def cleanUp(): Unit = {
      _heapDumps.foreach(_.toFile.delete())
    }
  }

  case class HeapDumpAtOffsetInputDataStream(inner: InputDataStream, triggerOffsets: Set[Long])
    extends OnNextInputDataStream(inner) with HeapDumpRecorder {

    override protected def onNext(offset: Long): Unit = if (triggerOffsets.contains(offset)) onOffset(offset)

    protected def onOffset(offset: Long): Unit = {
      val path = takeDump(offset.toString)
      debug(s"dump @ $offset (input): $path")
    }
  }

  case class HeapDumpAtIntervalsInputDataStream(stream: InputDataStream, interval: Long)
    extends OnIntervalInputDataStream(stream, interval) with HeapDumpRecorder {

    override protected def onInterval(offset: Long): Unit = {
      val path = takeDump(offset.toString)
      debug(s"dump @ $offset (input): $path")
    }
  }

  case class OffsetTrackingInputDataStream(stream: InputDataStream)
    extends OnNextInputDataStream(stream) {
    def currentOffset: Long = offset
    override protected def onNext(offset: Long): Unit = ()
  }

  case class HeapDumpAtIntervalsQuerySubscriber(inner: QuerySubscriber, interval: Long)
    extends OnIntervalQuerySubscriber(inner, interval) with HeapDumpRecorder {

    override protected def onInterval(offset: Long): Unit = {
      val path = takeDump(offset.toString)
      debug(s"dump @ $offset (output): $path")
    }
  }


  /**
   * Decorates another memory tracker to trigger a heap dump when target high water mark is reached
   */
  case class HighWaterMarkHeapDumpTrackerDecorator(targetHighWaterMark: Long) extends MemoryTrackerDecorator {
    private var tracker: HighWaterMarkHeapDumpTracker = _
    def recorder: HeapDumpRecorder = tracker
    override def apply(txTracker: MemoryTracker): MemoryTracker = {
      val queryScoped = new HighWaterScopedMemoryTracker(txTracker)
      tracker = HighWaterMarkHeapDumpTracker(queryScoped, targetHighWaterMark)
      tracker
    }
  }

  case class HighWaterMarkHeapDumpTracker(inner: MemoryTracker, targetHighWaterMark: Long)
    extends HeapDumpingMemoryTracker(inner) with HeapDumpRecorder {

    private val path = filePath(targetHighWaterMark.toString)

    setHeapDumpAtHighWaterMark(targetHighWaterMark, path.toAbsolutePath.toString, true, true, (_: HeapDumpingMemoryTracker) => {
      recordDump(path)
      debug(s"dump @ $targetHighWaterMark (hwm): $path")
    })
  }

  abstract class OnHighWaterMarkTracker(inner: MemoryTracker)
    extends HighWaterScopedMemoryTracker(inner) {

    private var _highWaterMark: Long = 0L

    def onHighWaterMark(highWaterMark: Long): Unit

    override def allocateHeap(bytes: Long): Unit = {
      super.allocateHeap(bytes)
      if (heapHighWaterMark() > _highWaterMark) {
        _highWaterMark = heapHighWaterMark()
        onHighWaterMark(_highWaterMark)
      }
    }
  }

  case class HighWaterMarkReleaseHeapDumpTrackerDecorator(targetHighWaterMark: Long) extends MemoryTrackerDecorator {
    private var tracker: HighWaterMarkHeapDumpTracker = _
    def recorder: HeapDumpRecorder = tracker
    override def apply(txTracker: MemoryTracker): MemoryTracker = {
      val queryScoped = new HighWaterScopedMemoryTracker(txTracker)
      tracker = HighWaterMarkHeapDumpTracker(queryScoped, targetHighWaterMark)
      tracker
    }
  }

  case class HighWaterMarkReleaseHeapDumpTracker(inner: MemoryTracker, targetHighWaterMark: Long)
    extends HeapDumpingMemoryTracker(inner) with HeapDumpRecorder {

    var dumpOnRelease = false

    setCallbackAtHighWaterMark(targetHighWaterMark, _ => dumpOnRelease = true)

    override def releaseHeap(bytes: Long): Unit = {
      if (dumpOnRelease) {
        val path = takeDump(targetHighWaterMark.toString)
        debug(s"dump @ $targetHighWaterMark (hwm release): $path")
      }
      super.releaseHeap(bytes)
    }

  }



  // --- Side-effecting streaming ---------------------------------------------

  abstract class OnIntervalInputDataStream(inner: InputDataStream, sideEffectInterval: Long)
    extends OnNextInputDataStream(inner) {

    private val countdown = new CountDown(sideEffectInterval, sideEffectInterval)

    override protected def onNext(offset: Long): Unit = if (countdown.tick()) onInterval(offset)

    protected def onInterval(offset: Long): Unit
  }

  abstract class OnNextInputDataStream(inner: InputDataStream)
    extends InputDataStream {

    protected var offset = 0L

    override def nextInputBatch(): InputCursor = {
      Option(inner.nextInputBatch())
        .map(cursor => new InputCursor {
          override def next(): Boolean = {
            val next = cursor.next()
            if (next) onNext(offset)
            offset += 1
            next
          }
          override def value(offset: Int): AnyValue = cursor.value(offset)
          override def close(): Unit = cursor.close()
        })
        .orNull
    }

    protected def onNext(offset: Long): Unit
  }

  abstract class OnIntervalQuerySubscriber(inner: QuerySubscriber, sideEffectInterval: Long)
    extends OnNextQuerySubscriber(inner) {

    private val countdown = new CountDown(1L, sideEffectInterval)

    override protected def onNext(offset: Long): Unit = if (countdown.tick()) onInterval(offset)

    protected def onInterval(offset: Long): Unit
  }

  abstract class OnNextQuerySubscriber(inner: QuerySubscriber)
    extends QuerySubscriber {

    var offset = 0L

    override def onResult(numberOfFields: Int): Unit = inner.onResult(numberOfFields)
    override def onRecord(): Unit = {
      // triggering here rather than in onRecordCompleted to avoid letting clean-up happen
      onNext(offset)
      offset += 1
      inner.onRecord()
    }
    override def onField(offset: Int, value: AnyValue): Unit = inner.onField(offset, value)
    override def onError(throwable: Throwable): Unit = inner.onError(throwable)
    override def onRecordCompleted(): Unit = inner.onRecordCompleted()
    override def onResultCompleted(statistics: QueryStatistics): Unit = inner.onResultCompleted(statistics)

    protected def onNext(offset: Long): Unit
  }

  class CountDown(initial: Long, interval: Long) {
    private var countdown = initial
    def tick(): Boolean = {
      countdown -= 1
      if (countdown == 0) {
        countdown = interval
        true
      } else {
        false
      }
    }
  }

  object Record {
    def header: Seq[String] = Seq("name", "estimate", "measurement", "error", "errorFraction")
  }
  sealed trait Record {
    def fields: Iterator[Any]
  }
  case class MeasurementRecord(name: String, estimate: Long, measurement: Long, error: Long, errorFraction: Double) extends Record {
    def fields: Iterator[Any] = productIterator
  }
  case class ErrorRecord(name: String, error: Throwable) extends Record {
    def fields: Iterator[Any] = Iterator(name, "error")
  }
}
