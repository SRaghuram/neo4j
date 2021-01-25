/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.exceptions.InternalException

object VariablePredicates {

  val NO_PREDICATE_OFFSET: Int = -1

  def expressionSlotForPredicate(predicate: Option[VariablePredicate]): Int =
    predicate match {
      case None => NO_PREDICATE_OFFSET
      case Some(VariablePredicate(ExpressionVariable(offset, _), _)) => offset
      case Some(VariablePredicate(v, _)) =>
        throw new InternalException(s"Failure during physical planning: the expression slot of variable $v has not been allocated.")
    }
}
