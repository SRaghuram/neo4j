/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext;
import org.neo4j.values.virtual.MapValue;

/**
 * Interface implemented by compiled projection.
 */
public interface CompiledProjection
{
    /**
     * Performs a projection
     *
     * @param context the current context.
     * @param dbAccess used for accessing the database
     * @param params the parameters of the query
     */
    void project( ExecutionContext context, DbAccess dbAccess, MapValue params );
}
