/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, QuerySubscriber, TransactionalContext}
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.VirtualValues

/**
  * This class contains various ugliness needed to perform physical compilation
  * and then execute a query.
  */
class RuntimeTestSupport[CONTEXT <: RuntimeContext](val graphDb: GraphDatabaseService,
                                                    val edition: Edition[CONTEXT],
                                                    val workloadMode: Boolean
                                                   ) extends CypherFunSuite {

  private val cypherGraphDb = new GraphDatabaseCypherService(graphDb)
  private val lifeSupport = new LifeSupport
  private val resolver: DependencyResolver = cypherGraphDb.getDependencyResolver
  private val runtimeContextManager = edition.newRuntimeContextManager(resolver, lifeSupport)
  private val monitors = resolver.resolveDependency(classOf[Monitors])
  private val contextFactory = Neo4jTransactionalContextFactory.create(cypherGraphDb)

  def start(): Unit = {
    lifeSupport.init()
    lifeSupport.start()
  }

  def stop(): Unit = {
    lifeSupport.stop()
    lifeSupport.shutdown()
  }

  def run[RESULT](logicalQuery: LogicalQuery,
                  runtime: CypherRuntime[CONTEXT],
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber): RESULT = {
    DebugLog.log("RuntimeTestSupport.run(...)")
    run(compile(logicalQuery, runtime), input, resultMapper, subscriber)
  }

  def run[RESULT](executableQuery: ExecutionPlan,
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber): RESULT = {
    val txContext = beginTx()
    val queryContext = newQueryContext(txContext)
    val runtimeContext = newRuntimeContext(txContext, queryContext)

    val result = executableQuery.run(queryContext, doProfile = false, VirtualValues.EMPTY_MAP, prePopulateResults = true, input, subscriber)
    val assertAllReleased =
      if (!workloadMode) runtimeContextManager.assertAllReleased _ else () => ()
    resultMapper(runtimeContext, new ClosingRuntimeResult(result, txContext, queryContext.resources, assertAllReleased))
  }

  def compile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]): ExecutionPlan = {
    val txContext = beginTx()
    val runtimeContext = newRuntimeContext(txContext, newQueryContext(txContext))
    try {
      runtime.compileToExecutable(logicalQuery, runtimeContext)
    } finally {
      txContext.close(true) // also closes tx
    }
  }

  private def beginTx(): TransactionalContext = {
    val tx = cypherGraphDb.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    contextFactory.newContext(tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
  }

  private def newRuntimeContext(txContext: TransactionalContext, queryContext: QueryContext): CONTEXT = {
    runtimeContextManager.create(queryContext,
                                 txContext.kernelTransaction().schemaRead(),
                                 MasterCompiler.CLOCK,
                                 Set.empty,
                                 compileExpressions = false)
  }

  private def newQueryContext(txContext: TransactionalContext): QueryContext = {
    new TransactionBoundQueryContext(TransactionalContextWrapper(txContext))(monitors.newMonitor(classOf[IndexSearchMonitor]))
  }
}
