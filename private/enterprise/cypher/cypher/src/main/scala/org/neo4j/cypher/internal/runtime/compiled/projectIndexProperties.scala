/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.opencypher.v9_0.util.attribution.SameId
import org.opencypher.v9_0.util.symbols.CTNode
import org.opencypher.v9_0.util.{Rewriter, topDown}

/**
  * Replace index plans that have indexed properties with `GetValue` by plans
  * that have `DoNotGetValue` instead, with a projection to get the values on
  * top of the index plan.
  */
case object projectIndexProperties {

  def apply(plan: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, SemanticTable) = {
    var currentTypes = semanticTable.types

    val rewriter = topDown(Rewriter.lift {
      case indexLeafPlan: IndexLeafPlan if indexLeafPlan.cachedNodeProperties.nonEmpty =>
        val projections = indexLeafPlan.availableCachedNodeProperties.map {
          case (prop, cachedNodeProperty) => (cachedNodeProperty.cacheKey, prop)
        }
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
