/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.spi.v3_5

import org.neo4j.cypher.internal.planner.v4_0.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.v4_0.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.v4_0.spi.StatisticsCompletingGraphStatistics
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.{Read, SchemaRead}
import org.neo4j.kernel.impl.query.TransactionalContext
import org.opencypher.v9_0.util.{Cardinality, LabelId, RelTypeId, Selectivity}

object TransactionBoundGraphStatistics {
  def apply(transactionalContext: TransactionalContext): StatisticsCompletingGraphStatistics =
    apply(transactionalContext.kernelTransaction().dataRead(), transactionalContext.kernelTransaction().schemaRead())

  def apply(read: Read, schemaRead: SchemaRead): StatisticsCompletingGraphStatistics =
    new StatisticsCompletingGraphStatistics(new BaseTransactionBoundGraphStatistics(read, schemaRead))

  private class BaseTransactionBoundGraphStatistics(read: Read, schemaRead: SchemaRead) extends GraphStatistics with IndexDescriptorCompatibility {

    override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] =
      try {
        val indexSize = schemaRead.indexSize(schemaRead.indexReferenceUnchecked(index.label, index.properties.map(_.id):_*))
        if (indexSize == 0)
          Some(Selectivity.ZERO)
        else {
          // Probability of any node in the index, to have a property with a given value
          val indexEntrySelectivity = schemaRead.indexUniqueValuesSelectivity(
            schemaRead.indexReferenceUnchecked(index.label, index.properties.map(_.id):_*))
          val frequencyOfNodesWithSameValue = 1.0 / indexEntrySelectivity

          // This is = 1 / number of unique values
          val indexSelectivity = frequencyOfNodesWithSameValue / indexSize

          Selectivity.of(indexSelectivity)
        }
      }
      catch {
        case _: IndexNotFoundKernelException => None
      }

    override def indexPropertyExistsSelectivity(index: IndexDescriptor): Option[Selectivity] =
      try {
        val labeledNodes = read.countsForNodeWithoutTxState( index.label ).toDouble
        if (labeledNodes == 0)
          Some(Selectivity.ZERO)
        else {
          // Probability of any node with the given label, to have a given property
          val indexSize = schemaRead.indexSize(schemaRead.indexReferenceUnchecked(index.label, index.properties.map(_.id):_*))
          val indexSelectivity = indexSize / labeledNodes

          Selectivity.of(indexSelectivity)
        }
      }
      catch {
        case e: IndexNotFoundKernelException => None
      }

    override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality =
      atLeastOne(read.countsForNodeWithoutTxState(labelId))

    override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality =
      atLeastOne(read.countsForRelationshipWithoutTxState(fromLabel, relTypeId, toLabel))

    /**
      * Due to the way cardinality calculations work, zero is a bit dangerous, as it cancels out
      * any cost that it multiplies with. To avoid this pitfall, we determine that the least count
      * available is one, not zero.
      */
    private def atLeastOne(count: Double): Cardinality = {
      if (count < 1)
        Cardinality.SINGLE
      else
        Cardinality(count)
    }

    override def nodesAllCardinality(): Cardinality = atLeastOne(read.countsForNodeWithoutTxState(-1))
  }
}
