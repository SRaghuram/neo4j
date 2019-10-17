/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.planner.v3_5.spi._
import org.neo4j.cypher.internal.runtime.interpreted.{LastCommittedTxIdProvider, TransactionBoundTokenContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v3_5.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory

import scala.collection.mutable

/**
  * This PlanContext is used for injecting customer statistics into the planner, which can help in
  * reproducing bug and support cases. It is not fit for production use. You can plug it into the
  * product, _temporarily_, overriding the var [[org.neo4j.cypher.internal.compatibility.v3_5.Cypher35Planner]].customPlanContextCreator
  * with an instance of this class in your test.
  *
  * @param data The parsed graph counts data.
  */
class GraphCountsPlanContext(data: GraphCountData)(tc: TransactionalContextWrapper, logger: InternalNotificationLogger)
  extends TransactionBoundTokenContext(tc.kernelTransaction) with PlanContext {

  private val indexes = data.indexes
  private val userDefinedFunctions = mutable.Map.empty[QualifiedName, UserFunctionSignature]

  def addUDF(udf: UserFunctionSignature): Unit = {
    userDefinedFunctions.put(udf.name, udf)
  }

  override val statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
    new StatisticsCompletingGraphStatistics(Stats(data.nodes, data.relationships)),
    new MutableGraphStatisticsSnapshot())

  override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexes
      .filter(_.labels.contains(getLabelName(labelId)))
      .map(x => IndexDescriptor(LabelId(labelId),
                                x.properties.map(propertyName => PropertyKeyId(getPropertyKeyId(propertyName))))
      ).toIterator
  }

  override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId)
  }

  override def indexExistsForLabel(labelId: Int): Boolean = {
    indexesGetForLabel(labelId).nonEmpty
  }

  override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] =
    indexes
      .filter(x => x.labels.contains(labelName) && x.properties == propertyKeys)
      .map(x => IndexDescriptor(LabelId(getLabelId(labelName)),
                                x.properties.map(propertyName => PropertyKeyId(getPropertyKeyId(propertyName))))
      ).headOption

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    indexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
   try {
      val labelId = getLabelId(labelName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      tc.schemaRead.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def checkNodeIndex(idxName: String) {
    if (!tc.kernelTransaction.indexRead().nodeExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  override def checkRelIndex(idxName: String) {
    if (!tc.kernelTransaction.indexRead().relationshipExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  case class Stats(nodes: Seq[NodeCount], relationships: Seq[RelationshipCount]) extends GraphStatistics {

    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      labelId match {
        case None => Cardinality(1)
        case Some(id) =>
          val nodeLabel = nodes.find(_.label.contains(getLabelName(id))).get
          Cardinality(nodeLabel.count)
      }

    override def nodesAllCardinality(): Cardinality = {
      val nodeLabel = nodes.find(_.label.isEmpty).get
      Cardinality(nodeLabel.count)
    }

    override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId],
                                                        relTypeId: Option[RelTypeId],
                                                        toLabel: Option[LabelId]): Cardinality = {
      val fromName = fromLabel.map(x => getLabelName(x.id))
      val relName = relTypeId.map(x => getRelTypeName(x.id))
      val toName = toLabel.map(x => getLabelName(x.id))

      if (relTypeId.isEmpty) {
        val relDefs = relationships.filter(r =>
                                           r.startLabel == fromName &&
                                           r.endLabel == toName)

        Cardinality(relDefs.map(_.count).sum)

      } else {
        val count = relationships.find(r =>
          r.relationshipType == relName &&
            r.startLabel == fromName &&
            r.endLabel == toName)
          .map(_.count).getOrElse(0L)
        Cardinality(count)
      }
    }

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
      indexes.find(x =>
                     x.labels.contains(getLabelName(index.label)) &&
                       x.properties == index.properties.map(x => getPropertyKeyName(x.id))
      ).map(x => if(x.estimatedUniqueSize == 0L) Selectivity.ZERO else Selectivity(1.0 / x.estimatedUniqueSize))

    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] = {
      val labelCardinality = nodesWithLabelCardinality(Some(index.label))
      indexes.find(x =>
                     x.labels.contains(getLabelName(index.label)) &&
                     x.properties == index.properties.map(x => getPropertyKeyName(x.id))
      ).map(x => Selectivity(x.totalSize / labelCardinality.amount))
    }
  }

  override val txIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def notificationLogger(): InternalNotificationLogger = logger

  override def procedureSignature(name: QualifiedName): ProcedureSignature = ???

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = userDefinedFunctions.get(name)
}
