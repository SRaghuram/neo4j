package org.neo4j.cypher.internal.runtime.spec.morsel

import org.neo4j.cypher.internal.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.BaseReactiveResultTest
import org.neo4j.cypher.internal.runtime.spec.{ENTERPRISE, Edition, LogicalQueryBuilder, RuntimeTestSuite}
import org.neo4j.cypher.internal.runtime.zombie.ZombieRuntime
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

class ZombieReactiveSingleThreadedTest extends ZombieReactiveResultTest(ENTERPRISE.SINGLE_THREADED_NO_FUSING)
class ZombieReactiveParallelTest extends ZombieReactiveResultTest(ENTERPRISE.PARALLEL_NO_FUSING)

abstract class ZombieReactiveResultTest(edition: Edition[EnterpriseRuntimeContext]) extends RuntimeTestSuite(edition, ZombieRuntime) with BaseReactiveResultTest {
  override def runtimeResult(subscriber: QuerySubscriber,
                             first: Array[AnyValue],
                             more: Array[AnyValue]*): RuntimeResult ={
    //move me
    val vars = (1 to first.length).map(i => s"v$i")
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(vars:_*)
      .input(variables = vars)
      .build()

    val builder = ArrayBuffer.empty[Array[AnyValue]]
    builder.append(first)
    for ( array <- more) builder.append(array)

    val input = inputValues(builder:_*)

    val runtimeResult: RuntimeResult = execute(logicalQuery, runtime, input, subscriber)
    runtimeResult
  }
}
