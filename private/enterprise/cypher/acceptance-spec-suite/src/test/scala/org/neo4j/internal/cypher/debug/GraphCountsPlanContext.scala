/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.internal.cypher.debug

import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.OrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.ValueCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateTimeType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.GeometryType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.TimeType
import org.neo4j.exceptions.KernelException
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexOrderCapability.ASC_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.ASC_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.BOTH_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.BOTH_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.DESC_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.DESC_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.NONE
import org.neo4j.internal.schema.IndexValueCapability
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider
import org.neo4j.values.storable.ValueCategory

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

/**
 * This PlanContext is used for injecting customer statistics into the planner, which can help in
 * reproducing bug and support cases. It is not fit for production use. You can plug it into the
 * product, _temporarily_, overriding the var [[org.neo4j.cypher.internal.planning.CypherPlanner]].customPlanContextCreator
 * with an instance of this class in your test.
 *
 * @param data The parsed graph counts data.
 */
class GraphCountsPlanContext(data: GraphCountData)(tc: TransactionalContextWrapper, logger: InternalNotificationLogger)
  extends TransactionBoundTokenContext(tc.kernelTransaction) with PlanContext {

  private val indexes = data.indexes
  private val userDefinedFunctions = mutable.Map.empty[QualifiedName, UserFunctionSignature]

  // Assume native index
  private val indexCapability: IndexCapability = GenericNativeIndexProvider.CAPABILITY

  // Same as product code in TransactionBoundPlanContext
  private val orderCapability: OrderCapability = tps => {
    indexCapability.orderCapability(tps.map(typeToValueCategory): _*) match {
      case BOTH_FULLY_SORTED => IndexOrderCapability.BOTH
      case BOTH_PARTIALLY_SORTED => IndexOrderCapability.BOTH
      case ASC_FULLY_SORTED => IndexOrderCapability.ASC
      case ASC_PARTIALLY_SORTED => IndexOrderCapability.ASC
      case DESC_FULLY_SORTED => IndexOrderCapability.DESC
      case DESC_PARTIALLY_SORTED => IndexOrderCapability.DESC
      case NONE => IndexOrderCapability.NONE
    }
  }
  private val valueCapability: ValueCapability = tps => {
    indexCapability.valueCapability(tps.map(typeToValueCategory): _*) match {
      // As soon as the kernel provides an array of IndexValueCapability, this mapping can change
      case IndexValueCapability.YES => tps.map(_ => CanGetValue)
      case IndexValueCapability.PARTIAL => tps.map(_ => DoNotGetValue)
      case IndexValueCapability.NO => tps.map(_ => DoNotGetValue)
    }
  }

  def addUDF(udf: UserFunctionSignature): Unit = {
    userDefinedFunctions.put(udf.name, udf)
  }

  override val statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
    new MinimumGraphStatistics(Stats(data.nodes, data.relationships)),
    new MutableGraphStatisticsSnapshot())

  override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexes
      .filter(_.labels.contains(getLabelName(labelId)))
      .map(x => IndexDescriptor(LabelId(labelId),
        x.properties.map(propertyName => PropertyKeyId(getPropertyKeyId(propertyName))),
                                orderCapability = orderCapability, valueCapability = valueCapability)
      ).toIterator
  }

  private def typeToValueCategory(in: CypherType): ValueCategory = in match {
    case _: IntegerType |
         _: FloatType =>
      ValueCategory.NUMBER

    case _: StringType =>
      ValueCategory.TEXT

    case _: GeometryType | _: PointType =>
      ValueCategory.GEOMETRY

    case _: DateTimeType | _: LocalDateTimeType | _: DateType | _: TimeType | _: LocalTimeType | _: DurationType =>
      ValueCategory.TEMPORAL

    // For everything else, we don't know
    case _ =>
      ValueCategory.UNKNOWN
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
        x.properties.map(propertyName => PropertyKeyId(getPropertyKeyId(propertyName))),
                                orderCapability = orderCapability, valueCapability = valueCapability)
      ).headOption

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    indexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
    try {
      val labelId = getLabelId(labelName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      tc.schemaRead.constraintsGetForSchema(SchemaDescriptor.forLabel(labelId, propertyKeyId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def getPropertiesWithExistenceConstraint(labelName: String): Set[String] = {
    try {
      val labelId = getLabelId(labelName)

      val constraints: Iterator[ConstraintDescriptor] = tc.schemaRead.constraintsGetForLabel(labelId).asScala

      // We are only interested of existence and node key constraints, not unique constraints
      val existsConstraints = constraints.filter(c => c.enforcesPropertyExistence())

      // Fetch the names of all unique properties that are part of at least one existence/node key constraint with the given label
      // i.e. the name of all properties that a node with the given label must have
      val distinctPropertyIds: Set[Int] = existsConstraints.flatMap(_.schema().getPropertyIds).toSet
      distinctPropertyIds.map(id => tc.tokenRead.propertyKeyName(id))
    } catch {
      case _: KernelException => Set.empty
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

    override def patternStepCardinality(fromLabel: Option[LabelId],
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
