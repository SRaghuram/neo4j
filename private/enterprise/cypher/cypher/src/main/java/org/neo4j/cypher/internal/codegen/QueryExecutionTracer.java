/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.codegen;

import org.neo4j.cypher.internal.v4_0.util.attribution.Id;

import org.neo4j.cypher.internal.runtime.compiled.codegen.QueryExecutionEvent;

public interface QueryExecutionTracer
{
    QueryExecutionEvent executeOperator( Id queryId );

    QueryExecutionTracer NONE = queryId -> QueryExecutionEvent.NONE;
}
