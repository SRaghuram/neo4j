/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.slotted.pipes

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.JavaConverters.asScalaBufferConverter

class ProduceResultSlottedPipeStressTest extends CypherFunSuite {

  private val column = "x"
  private val slotConfig = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)

  test(s"concurrent slotted produce result execution should not crash") {

    // Given
    val produceResults = ProduceResultSlottedPipe(sourcePipe, List(column -> literal(42)))(Id.INVALID_ID)
    val expected = execute(produceResults)

    // When
    val nThreads = 10
    val executor = Executors.newFixedThreadPool(nThreads)
    val futureResultsAsExpected =
      for (_ <- 1 to nThreads) yield
        executor.submit(new Callable[Array[AnyValue]] {
          override def call(): Array[AnyValue] = {
            (for (_ <- 1 to 1000) yield {
              execute(produceResults)
            }).toArray
          }
        })

    // Then no crashes...
    for (futureResultSet: Future[Array[AnyValue]] <- futureResultsAsExpected;
         result: AnyValue <- futureResultSet.get
         ) {
      // ...and correct results
      result should equal(expected)
    }
    executor.shutdown()
  }

  private def execute(produceResults: ProduceResultSlottedPipe): AnyValue = {
    val subscriber = new RecordingQuerySubscriber
    subscriber.onResult(1)
    val iterator = produceResults.createResults(QueryStateHelper.emptyWith(subscriber = subscriber))
    subscriber.onResultCompleted(QueryStatistics.empty)
    //equivalent of request(1)
    iterator.next()

    subscriber.getOrThrow().asScala.head(0)
  }

  private val sourcePipe: Pipe =
    new Pipe {
      override protected def internalCreateResults(state: QueryState): Iterator[CypherRow] =
        Iterator(SlottedRow(slotConfig))
      override val id: Id = Id.INVALID_ID
    }
}
