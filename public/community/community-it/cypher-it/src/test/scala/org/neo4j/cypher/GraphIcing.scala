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
package org.neo4j.cypher

import java.util
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.{RuntimeJavaValueConverter, isGraphKernelResultValue}
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb._
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query._
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.util.ValueUtils.asMapValue

import scala.collection.JavaConverters._

trait GraphIcing {

  implicit class RichNode(n: Node) {
    def labels: List[String] = n.getLabels.asScala.map(_.name()).toList

    def addLabels(input: String*) {
      input.foreach(l => n.addLabel(label(l)))
    }
  }

  implicit class RichGraphDatabaseQueryService(graphService: GraphDatabaseQueryService) {

    private val graph: GraphDatabaseFacade = graphService.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService

    def getAllNodes() = graph.getAllNodes

    def getAllRelationships() = graph.getAllRelationships

    def getAllRelationshipTypes() = graph.getAllRelationshipTypes

    def schema() = graph.schema

    def execute(query: String) = graph.execute(query)

    def execute(query: String, params: util.Map[String, Object]) = graph.execute(query, params)

    def indexPropsForLabel(label: String): List[List[String]] = {
      val indexDefs = graph.schema.getIndexes(Label.label(label)).asScala.toList
      indexDefs.map(_.getPropertyKeys.asScala.toList)
    }

    def createUniqueConstraint(label: String, property: String) = {
      inTx {
        graph.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property).create()
      }
    }

    def createExistenceConstraint(label: String, property: String) = {
      inTx {
        graph.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT exists(n.$property)")
      }
    }

    def createNodeKeyConstraint(label: String, properties: String*): Result = {
      inTx {
        graph.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.${properties.mkString(", n.")}) IS NODE KEY")
      }
    }

    def createIndex(label: String, properties: String*): IndexDefinition = {
      inTx {
        graph.execute(s"CREATE INDEX ON :$label(${properties.map(p => s"`$p`").mkString(",")})")
      }

      inTx {
        graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      }

      getIndex(label, properties)
    }

    def getIndex(label: String, properties: Seq[String]): IndexDefinition = {
      inTx {
        graph.schema().getIndexes(Label.label(label)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList).get
      }
    }

    def createUniqueIndex(label: String, property: String): Unit = {
      inTx {
        graph.execute(s"CREATE CONSTRAINT ON (p:$label) ASSERT p.$property IS UNIQUE")
      }

      inTx {
        graph.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      }
    }

    def statement: Statement = txBridge.get(graph.databaseId())

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def inTx[T](f: InternalTransaction => T, txType: Type = Type.`implicit`): T = withTx(f, txType)

    def inTx[T](f: => T): T = inTx(_ => f)

    private val javaValues = new RuntimeJavaValueConverter(isGraphKernelResultValue)

    private def createTransactionalContext(tx: InternalTransaction, queryText: String, params: Map[String, Any] = Map.empty): TransactionalContext = {
      val javaParams = javaValues.asDeepJavaMap(params).asInstanceOf[util.Map[String, AnyRef]]
      val contextFactory = Neo4jTransactionalContextFactory.create(graphService)
      contextFactory.newContext(tx, queryText, asMapValue(javaParams))
    }

    def transactionalContext(tx: InternalTransaction, query: (String, Map[String, Any])): TransactionalContext = {
      val (queryText, params) = query
      createTransactionalContext(tx, queryText, params)
    }

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def withTx[T](f: InternalTransaction => T, txType: Type = Type.`implicit`): T = {
      val tx = graph.beginTransaction(txType, AUTH_DISABLED)
      try {
        val result = f(tx)
        tx.commit()
        result
      } finally {
        tx.close()
      }

    }

    def rollback[T](f: InternalTransaction => T): T = {
      val tx = graph.beginTransaction(Type.`implicit`, AUTH_DISABLED)
      try {
        val result = f(tx)
        tx.rollback()
        result
      } finally {
        tx.close()
      }
    }

    def txCounts = TxCounts(txMonitor.getNumberOfCommittedTransactions, txMonitor.getNumberOfRolledBackTransactions, txMonitor.getNumberOfActiveTransactions)

    private def txMonitor: DatabaseTransactionStats = graph.getDependencyResolver.resolveDependency(classOf[DatabaseTransactionStats])

    private def txBridge: ThreadToStatementContextBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }
}

final case class TxCounts(commits: Long = 0, rollbacks: Long = 0, active: Long = 0) {
  def +(other: TxCounts) = TxCounts(commits + other.commits, rollbacks + other.rollbacks, active + other.active)
  def -(other: TxCounts) = TxCounts(commits - other.commits, rollbacks - other.rollbacks, active - other.active)
}
