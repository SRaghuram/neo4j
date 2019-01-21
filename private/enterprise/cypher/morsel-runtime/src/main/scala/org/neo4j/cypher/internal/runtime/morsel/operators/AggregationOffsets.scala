/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.operators

import org.neo4j.cypher.internal.runtime.morsel.expressions.AggregationExpressionOperator

/**
  *
  * @param mapperOutputSlot
  * Slot from AggregationMapper's pipeline where aggregation result is written to by the Mapper and read from by the Reducer.
  * @param reducerOutputSlot
  * Slot from AggregationReducers's pipeline where grouping is written to by the Reducer.
  * @param aggregation
  * Aggregation expression to be operated on by the Mapper.
  */
case class AggregationOffsets(mapperOutputSlot: Int, reducerOutputSlot: Int, aggregation: AggregationExpressionOperator)
