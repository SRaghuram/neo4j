/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.executionplan;

import org.neo4j.cypher.internal.codegen.QueryExecutionTracer;
import org.neo4j.cypher.internal.runtime.ExecutionMode;
import org.neo4j.cypher.internal.runtime.QueryContext;
import org.neo4j.values.virtual.MapValue;

public interface GeneratedQuery
{
    GeneratedQueryExecution execute(
            QueryContext queryContext,
            QueryExecutionTracer tracer,
            MapValue params );
}
