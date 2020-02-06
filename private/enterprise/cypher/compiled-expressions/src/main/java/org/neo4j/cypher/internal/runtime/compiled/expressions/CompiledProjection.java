/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.values.AnyValue;

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
     * @param cursors cursors to use for expression evaluation
     * @param expressionVariables array used for storing expression variable values
     */
    void project( CypherRow context,
                  DbAccess dbAccess,
                  AnyValue[] params,
                  ExpressionCursors cursors,
                  AnyValue[] expressionVariables );
}
