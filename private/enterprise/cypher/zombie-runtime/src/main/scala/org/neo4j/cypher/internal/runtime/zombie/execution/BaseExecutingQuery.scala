//package org.neo4j.cypher.internal.runtime.zombie.execution
//
//import org.neo4j.cypher.internal.runtime.QueryContext
//import org.neo4j.cypher.internal.runtime.morsel.QueryState
//import org.neo4j.cypher.internal.runtime.scheduling.QueryExecutionTracer
//import org.neo4j.cypher.internal.runtime.zombie.ExecutionState
//import org.neo4j.kernel.impl.query.QuerySubscription
//
//abstract class BaseExecutingQuery extends QuerySubscription {
//
//  def executionState: ExecutionState
//
//  def queryContext: QueryContext
//
//  def queryState: QueryState
//
//  def queryExecutionTracer: QueryExecutionTracer
//
//  final def bindTransactionToThread(): Unit =
//    queryState.transactionBinder.bindToThread(queryContext.transactionalContext.transaction)
//
//  final def unbindTransaction(): Unit =
//    queryState.transactionBinder.unbindFromThread()
//}
