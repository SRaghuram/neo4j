/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.IndexInfo
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.Nonunique
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.Unique
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowIndexesCommand.createIndexStatement
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.asEscapedString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.btreeConfigValueAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.colonStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.configAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.escapeBackticks
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.extractOptionsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.optionsAsString
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.propStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowSchemaCommandHelper.relPropStringJoiner
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.util.StringJoiner
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.immutable.ListMap

// SHOW [ALL|BTREE] INDEX[ES] [BRIEF|VERBOSE|WHERE clause|YIELD clause]
case class ShowIndexesCommand(indexType: ShowIndexType, verbose: Boolean, columns: Set[ShowColumn]) extends Command(columns) {
  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query
    ctx.assertShowIndexAllowed()
    val indexes: Map[IndexDescriptor, IndexInfo] = ctx.getAllIndexes()
    val relevantIndexes = indexType match {
      case AllIndexes => indexes
      case BtreeIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.BTREE)
        }
      case FulltextIndexes =>
        indexes.filter {
          case (indexDescriptor, _) => indexDescriptor.getIndexType.equals(IndexType.FULLTEXT)
        }
    }

    val sortedRelevantIndexes: ListMap[IndexDescriptor, IndexInfo] = ListMap(relevantIndexes.toSeq.sortBy(_._1.getName): _*)
    val rows = sortedRelevantIndexes.map {
      case (indexDescriptor: IndexDescriptor, indexInfo: IndexInfo) =>
        val indexStatus = indexInfo.indexStatus
        val uniqueness = if (indexDescriptor.isUnique) Unique.toString else Nonunique.toString
        val indexType = indexDescriptor.getIndexType

        /*
         * For btree the create command is the Cypher CREATE INDEX which needs the name to be escaped,
         * in case it contains special characters.
         * For fulltext the create command is a procedure which doesn't require escaping.
        */
        val name = indexDescriptor.getName
        val escapedName = escapeBackticks(name)
        val createName = if (indexType.equals(IndexType.BTREE)) s"`$escapedName`" else name

        val entityType = indexDescriptor.schema.entityType
        val labels = indexInfo.labelsOrTypes
        val properties = indexInfo.properties
        val providerName = indexDescriptor.getIndexProvider.name

        val briefResult = Map(
          // The id of the index
          "id" -> Values.longValue(indexDescriptor.getId),
          // Name of the index, for example "myIndex"
          "name" -> Values.stringValue(escapedName),
          // Current state of the index, one of "ONLINE", "FAILED", "POPULATING"
          "state" -> Values.stringValue(indexStatus.state),
          // % of index population, for example 0.0, 100.0, or 75.1
          "populationPercent" -> Values.doubleValue(indexStatus.populationProgress),
          // Tells if the index is only meant to allow one value per key, either "UNIQUE" or "NONUNIQUE"
          "uniqueness" -> Values.stringValue(uniqueness),
          // The IndexType of this index, either "FULLTEXT" or "BTREE"
          "type" -> Values.stringValue(indexType.name),
          // Type of entities this index represents, either "NODE" or "RELATIONSHIP"
          "entityType" -> Values.stringValue(entityType.name),
          // The labels or relationship types of this constraint, for example ["Label1", "Label2"] or ["RelType1", "RelType2"]
          "labelsOrTypes" -> VirtualValues.fromList(labels.map(elem => Values.of(elem).asInstanceOf[AnyValue]).asJava),
          // The properties of this constraint, for example ["propKey", "propKey2"]
          "properties" -> VirtualValues.fromList(properties.map(prop => Values.of(prop).asInstanceOf[AnyValue]).asJava),
          // The index provider for this index, one of "native-btree-1.0", "lucene+native-3.0", "fulltext-1.0"
          "indexProvider" -> Values.stringValue(providerName)
        )
        if (verbose) {
          val indexConfig = indexDescriptor.getIndexConfig
          briefResult ++ Map(
            "options" -> extractOptionsMap(providerName, indexConfig),
            "failureMessage" -> Values.stringValue(indexInfo.indexStatus.failureMessage),
            "createStatement" -> Values.stringValue(
              createIndexStatement(createName, indexType, entityType, labels, properties, providerName, indexConfig, indexStatus.maybeConstraint))
          )
        } else {
          briefResult
        }
    }
    ClosingIterator.apply(rows.iterator)
  }

}

object ShowIndexesCommand {
  sealed trait Uniqueness

  case object Unique extends Uniqueness {
    override final val toString: String = "UNIQUE"
  }

  case object Nonunique extends Uniqueness {
    override final val toString: String = "NONUNIQUE"
  }

  private def createIndexStatement(escapedName: String,
                                   indexType: IndexType,
                                   entityType: EntityType,
                                   labelsOrTypes: List[String],
                                   properties: List[String],
                                   providerName: String,
                                   indexConfig: IndexConfig,
                                   maybeConstraint: Option[ConstraintDescriptor]): String = {

    indexType match {
      case IndexType.BTREE =>
        val labelsOrTypesWithColons = asEscapedString(labelsOrTypes, colonStringJoiner)
        val escapedNodeProperties = asEscapedString(properties, propStringJoiner)

        val btreeConfig = configAsString(indexConfig, value => btreeConfigValueAsString(value))
        val optionsString = optionsAsString(providerName, btreeConfig)

        maybeConstraint match {
          case Some(constraint) if constraint.isUniquenessConstraint =>
            s"CREATE CONSTRAINT $escapedName ON (n$labelsOrTypesWithColons) ASSERT ($escapedNodeProperties) IS UNIQUE OPTIONS $optionsString"
          case Some(constraint) if constraint.isNodeKeyConstraint =>
            s"CREATE CONSTRAINT $escapedName ON (n$labelsOrTypesWithColons) ASSERT ($escapedNodeProperties) IS NODE KEY OPTIONS $optionsString"
          case Some(_) =>
            throw new IllegalArgumentException("Expected an index or index backed constraint, found another constraint.")
          case None =>
            entityType match {
              case EntityType.NODE =>
                s"CREATE INDEX $escapedName FOR (n$labelsOrTypesWithColons) ON ($escapedNodeProperties) OPTIONS $optionsString"
              case EntityType.RELATIONSHIP =>
                val escapedRelProperties = asEscapedString(properties, relPropStringJoiner)
                s"CREATE INDEX $escapedName FOR ()-[r$labelsOrTypesWithColons]-() ON ($escapedRelProperties) OPTIONS $optionsString"
              case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
            }
        }
      case IndexType.FULLTEXT =>
        val labelsOrTypesArray = asString(labelsOrTypes, arrayStringJoiner)
        val propertiesArray = asString(properties, arrayStringJoiner)
        val fulltextConfig = configAsString(indexConfig, value => fullTextConfigValueAsString(value))

        entityType match {
          case EntityType.NODE =>
            s"CALL db.index.fulltext.createNodeIndex('$escapedName', $labelsOrTypesArray, $propertiesArray, $fulltextConfig)"
          case EntityType.RELATIONSHIP =>
            s"CALL db.index.fulltext.createRelationshipIndex('$escapedName', $labelsOrTypesArray, $propertiesArray, $fulltextConfig)"
          case _ => throw new IllegalArgumentException(s"Did not recognize entity type $entityType")
        }
      case _ => throw new IllegalArgumentException(s"Did not recognize index type $indexType")
    }
  }

  private def asString(list: List[String], stringJoiner: StringJoiner): String = {
    for (elem <- list) {
      stringJoiner.add(s"'$elem'")
    }
    stringJoiner.toString
  }

  private def fullTextConfigValueAsString(configValue: Value): String = {
    configValue match {
      case booleanValue: BooleanValue => "'" + booleanValue.booleanValue() + "'"
      case stringValue: StringValue =>"'" + stringValue.stringValue() + "'"
      case _ => throw new IllegalArgumentException(s"Could not convert config value '$configValue' to config string.")
    }
  }

  private def arrayStringJoiner = new StringJoiner(", ", "[", "]")
}
