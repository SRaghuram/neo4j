/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.eval

import java.net.URL
import java.time.Clock
import java.util.function.Supplier

import org.eclipse.collections.api.iterator.LongIterator
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.evaluator.EvaluationException
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.NodeOperations
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.RelationshipIterator
import org.neo4j.cypher.internal.runtime.RelationshipOperations
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Path
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.AnyValue
import org.neo4j.values.ValueMapper

import scala.collection.Iterator

object StaticEvaluation {

  class StaticEvaluator(proceduresSupplier: Supplier[GlobalProcedures]) extends SimpleInternalExpressionEvaluator {
    override def queryState(nExpressionSlots: Int, slottedParams: Array[AnyValue]) = new QueryState(
      query = new StaticQueryContext(proceduresSupplier.get()),
      resources = null,
      params = slottedParams,
      cursors = null,
      queryIndexes = Array.empty,
      expressionVariables = new Array(nExpressionSlots),
      subscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
      memoryTracker = null
    )

    override def evaluate(expression: Expression, params: MapValue, context: CypherRow): AnyValue = {
      try {
        super.evaluate(expression, params, context)
      } catch {
        case e: EvaluationException =>
          // all errors in expression evaluation are wrapped in generic EvaluationException,
          // let's see if there is a more interesting error wrapped in it (interesting means an error with a status code in this context) .
          var unwrapped: Throwable = e
          while (unwrapped.isInstanceOf[EvaluationException])
            unwrapped = unwrapped.getCause
          if (unwrapped != null && unwrapped.isInstanceOf[HasStatus]) {
            throw unwrapped
          } else {
            // there isn't an exception with a status wrapped in the EvaluationException,
            // so let's throw the origin exception
            throw e
          }
      }
    }
  }

  private class StaticQueryContext(procedures: GlobalProcedures) extends EmptyQueryContext {
    override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue =
      procedures.callFunction(new StaticProcedureContext, id, args)
  }

  private class StaticProcedureContext extends EmptyProcedureContext

  private def notAvailable(): Nothing =
    throw new RuntimeException("Operation not available in static context.")

  private trait EmptyProcedureContext extends Context {

    override def procedureCallContext(): ProcedureCallContext = notAvailable()

    override def valueMapper(): ValueMapper[AnyRef] = notAvailable()

    override def securityContext(): SecurityContext = notAvailable()

    override def dependencyResolver(): DependencyResolver = notAvailable()

    override def graphDatabaseAPI(): GraphDatabaseAPI = notAvailable()

    override def thread(): Thread = notAvailable()

    override def systemClock(): Clock = notAvailable()

    override def statementClock(): Clock = notAvailable()

    override def transactionClock(): Clock = notAvailable()

    override def internalTransaction(): InternalTransaction = notAvailable()

    override def internalTransactionOrNull(): InternalTransaction = notAvailable()
  }

  private trait EmptyQueryContext extends QueryContext {

    override def entityAccessor: TransactionalEntityFactory = notAvailable()

    override def transactionalContext: QueryTransactionalContext = notAvailable()

    override def resources: ResourceManager = notAvailable()

    override def nodeOps: NodeOperations = notAvailable()

    override def relationshipOps: RelationshipOperations = notAvailable()

    override def createNode(labels: Array[Int]): NodeValue = notAvailable()

    override def createNodeId(labels: Array[Int]): Long = notAvailable()

    override def createRelationship(start: Long, end: Long, relType: Int): RelationshipValue = notAvailable()

    override def getOrCreateRelTypeId(relTypeName: String): Int = notAvailable()

    override def getRelationshipsForIds(node: Long, dir: SemanticDirection, types: Array[Int]): Iterator[RelationshipValue] = notAvailable()

    override def nodeCursor(): NodeCursor = notAvailable()

    override def groupCursor(): RelationshipGroupCursor = notAvailable()

    override def traversalCursor(): RelationshipTraversalCursor = notAvailable()

    override def getRelationshipsForIdsPrimitive(node: Long, dir: SemanticDirection, types: Array[Int]): RelationshipIterator = notAvailable()

    override def relationshipById(id: Long, startNode: Long, endNode: Long, `type`: Int): RelationshipValue = notAvailable()

    override def getOrCreateLabelId(labelName: String): Int = notAvailable()

    override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int = notAvailable()

    override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int = notAvailable()

    override def getOrCreatePropertyKeyId(propertyKey: String): Int = notAvailable()

    override def getOrCreatePropertyKeyIds(propertyKeys: Array[String]): Array[Int] = notAvailable()

    override def addIndexRule(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): IndexDescriptor = notAvailable()

    override def dropIndexRule(labelId: Int, propertyKeyIds: Seq[Int]): Unit = notAvailable()

    override def dropIndexRule(name: String): Unit = notAvailable()

    override def indexReference(label: Int, properties: Int*): IndexDescriptor = notAvailable()

    override def indexSeek[RESULT <: AnyRef](index: IndexReadSession, needsValues: Boolean, indexOrder: IndexOrder, queries: Seq[IndexQuery]): NodeValueIndexCursor = notAvailable()

    override def indexSeekByContains[RESULT <: AnyRef](index: IndexReadSession, needsValues: Boolean, indexOrder: IndexOrder, value: TextValue): NodeValueIndexCursor = notAvailable()

    override def indexSeekByEndsWith[RESULT <: AnyRef](index: IndexReadSession, needsValues: Boolean, indexOrder: IndexOrder, value: TextValue): NodeValueIndexCursor = notAvailable()

    override def indexScan[RESULT <: AnyRef](index: IndexReadSession, needsValues: Boolean, indexOrder: IndexOrder): NodeValueIndexCursor = notAvailable()

    override def lockingUniqueIndexSeek[RESULT](index: IndexDescriptor, queries: Seq[IndexQuery.ExactPredicate]): NodeValueIndexCursor = notAvailable()

    override def getNodesByLabel(id: Int): Iterator[NodeValue] = notAvailable()

    override def getNodesByLabelPrimitive(id: Int): LongIterator = notAvailable()

    override def createNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit = notAvailable()

    override def dropNodeKeyConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = notAvailable()

    override def createUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int], name: Option[String]): Unit = notAvailable()

    override def dropUniqueConstraint(labelId: Int, propertyKeyIds: Seq[Int]): Unit = notAvailable()

    override def createNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int, name: Option[String]): Unit = notAvailable()

    override def dropNodePropertyExistenceConstraint(labelId: Int, propertyKeyId: Int): Unit = notAvailable()

    override def createRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int, name: Option[String]): Unit = notAvailable()

    override def dropRelationshipPropertyExistenceConstraint(relTypeId: Int, propertyKeyId: Int): Unit = notAvailable()

    override def dropNamedConstraint(name: String): Unit = notAvailable()

    override def getImportURL(url: URL): Either[String, URL] = notAvailable()

    override def nodeIsDense(node: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def asObject(value: AnyValue): AnyRef = notAvailable()

    override def variableLengthPathExpand(realNode: Long, minHops: Option[Int], maxHops: Option[Int], direction: SemanticDirection, relTypes: Seq[String]): Iterator[Path] = notAvailable()

    override def singleShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]]): Option[Path] = notAvailable()

    override def allShortestPath(left: Long, right: Long, depth: Int, expander: Expander, pathPredicate: KernelPredicate[Path], filters: Seq[KernelPredicate[Entity]]): Iterator[Path] = notAvailable()

    override def nodeCountByCountStore(labelId: Int): Long = notAvailable()

    override def relationshipCountByCountStore(startLabelId: Int, typeId: Int, endLabelId: Int): Long = notAvailable()

    override def lockNodes(nodeIds: Long*): Unit = notAvailable()

    override def lockRelationships(relIds: Long*): Unit = notAvailable()

    override def callReadOnlyProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = notAvailable()

    override def callReadWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = notAvailable()

    override def callSchemaWriteProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = notAvailable()

    override def callDbmsProcedure(id: Int, args: Seq[AnyValue], allowed: Array[String], context: ProcedureCallContext): Iterator[Array[AnyValue]] = notAvailable()

    override def aggregateFunction(id: Int, allowed: Array[String]): UserDefinedAggregator = notAvailable()

    override def detachDeleteNode(id: Long): Int = notAvailable()

    override def assertSchemaWritesAllowed(): Unit = notAvailable()

    override def getLabelName(id: Int): String = notAvailable()

    override def getOptLabelId(labelName: String): Option[Int] = None

    override def getLabelId(labelName: String): Int = notAvailable()

    override def getPropertyKeyName(id: Int): String = notAvailable()

    override def getOptPropertyKeyId(propertyKeyName: String): Option[Int] = None

    override def getPropertyKeyId(propertyKeyName: String): Int = notAvailable()

    override def getRelTypeName(id: Int): String = notAvailable()

    override def getOptRelTypeId(relType: String): Option[Int] = None

    override def getRelTypeId(relType: String): Int = notAvailable()

    override def nodeById(id: Long): NodeValue = notAvailable()

    override def relationshipById(id: Long): RelationshipValue = notAvailable()

    override def nodePropertyIds(node: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Array[Int] = notAvailable()

    override def propertyKey(name: String): Int = notAvailable()

    override def nodeLabel(name: String): Int = notAvailable()

    override def relationshipType(name: String): Int = notAvailable()

    override def relationshipTypeName(typ: Int): String = notAvailable()

    override def getTypeForRelationship(id: Long, relationshipCursor: RelationshipScanCursor): TextValue = notAvailable()

    override def nodeHasProperty(node: Long, property: Int, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): Boolean = notAvailable()

    override def relationshipPropertyIds(node: Long, relationshipScanCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): Array[Int] = notAvailable()

    override def relationshipHasProperty(node: Long, property: Int, relationshipScanCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): Boolean = notAvailable()

    override def nodeGetOutgoingDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetOutgoingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetIncomingDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetIncomingDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetTotalDegree(node: Long, nodeCursor: NodeCursor): Int = notAvailable()

    override def nodeGetTotalDegree(node: Long, relationship: Int, nodeCursor: NodeCursor): Int = notAvailable()

    override def singleRelationship(id: Long, cursor: RelationshipScanCursor): Unit = notAvailable()

    override def getLabelsForNode(id: Long, nodeCursor: NodeCursor): ListValue = notAvailable()

    override def isLabelSetOnNode(label: Int, id: Long, nodeCursor: NodeCursor): Boolean = notAvailable()

    override def nodeAsMap(id: Long, nodeCursor: NodeCursor, propertyCursor: PropertyCursor): MapValue = notAvailable()

    override def relationshipAsMap(id: Long, relationshipCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): MapValue = notAvailable()

    override def callFunction(id: Int, args: Array[AnyValue], allowed: Array[String]): AnyValue = notAvailable()

    override def getTxStateNodePropertyOrNull(nodeId: Long, propertyKey: Int): Value = notAvailable()

    override def getTxStateRelationshipPropertyOrNull(relId: Long, propertyKey: Int): Value = notAvailable()
  }

}
