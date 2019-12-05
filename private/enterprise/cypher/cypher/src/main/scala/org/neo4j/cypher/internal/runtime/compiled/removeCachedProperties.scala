/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.{CachedProperty, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.{InputPosition, Rewriter, topDown}

/**
  * Replace index plans that have indexed properties with `GetValue` by plans
  * that have `DoNotGetValue` instead, with a projection to get the values on
  * top of the index plan.
  *
  * Replace CachedProperties with Properties, since compiled does not support
  * CachedProperties.
  */
case object removeCachedProperties {

  def apply(plan: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, SemanticTable) = {
    var currentTypes = semanticTable.types

    val rewriter = topDown(Rewriter.lift {
      case cp@CachedProperty(_, varUsed, propertyKeyName, _) => Property(varUsed, propertyKeyName)(cp.position)

      case indexLeafPlan: IndexLeafPlan if indexLeafPlan.cachedProperties.nonEmpty =>
        val projections: Map[String, Property] = indexLeafPlan.cachedProperties.map { cachedProperty =>
          cachedProperty.propertyAccessString -> Property(
            Variable(cachedProperty.entityName)(InputPosition.NONE),
            cachedProperty.propertyKey
          )(InputPosition.NONE)
        }.toMap

        // Register all variables in the property lookups as nodes
        projections.values.foreach { prop =>
          currentTypes = currentTypes.updated(prop.map, ExpressionTypeInfo(CTNode.invariant, None))
        }

        val newIndexPlan = indexLeafPlan.copyWithoutGettingValues
        Projection(newIndexPlan, projections)(SameId(indexLeafPlan.id))
    })

    val rewrittenPlan = rewriter(plan).asInstanceOf[LogicalPlan]
    val newSemanticTable = if(currentTypes == semanticTable.types) semanticTable else semanticTable.copy(types = currentTypes)
    (rewrittenPlan, newSemanticTable)
  }
}
