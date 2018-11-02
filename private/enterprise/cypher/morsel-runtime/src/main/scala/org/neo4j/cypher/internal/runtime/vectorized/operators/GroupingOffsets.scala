/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.Slot
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression

/**
  *
  * @param mapperOutputSlot
  * Slot from AggregationMapper's pipeline where grouping is written to by the Mapper and read from by the Reducer.
  * @param reducerOutputSlot
  * Slot from AggregationReducers's pipeline where grouping is written to by the Reducer.
  * @param expression
  * Grouping expression to be operated on by the Mapper.
  */
case class GroupingOffsets(mapperOutputSlot: Slot, reducerOutputSlot: Slot, expression: Expression)
