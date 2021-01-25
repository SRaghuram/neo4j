/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction

class TxBatchWithSecurity(db: GraphDatabaseService, txBatchSize: Int, loginContext: LoginContext) extends TxBatch(db, txBatchSize) {
  val graph = new GraphDatabaseCypherService(db)

  override def transaction(): InternalTransaction = super.transaction().asInstanceOf[InternalTransaction]

  override protected def beginTransaction(): InternalTransaction = graph.beginTransaction(Type.EXPLICIT, loginContext)

}
