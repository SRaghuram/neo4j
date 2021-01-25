/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.cypher.internal.runtime.ReadableRow;
import org.neo4j.cypher.internal.runtime.WritableRow;
import org.neo4j.values.AnyValue;

/**
 * Interface implemented by compiled projection.
 */
public interface CompiledGroupingExpression
{
    /**
     * Projects the given grouping key to the given context
     * @param context the context to write to
     * @param groupingKey the grouping key to project
     */
    void projectGroupingKey( WritableRow context, AnyValue groupingKey );

    /**
     * Computes grouping key.
     * @param context the current context.
     * @param dbAccess used for accessing the database
     * @param params the parameters of the query
     * @param cursors cursors to use for expression evaluation
     * @param expressionVariables array used for storing expression variable values
     */
    AnyValue computeGroupingKey( ReadableRow context,
                                 DbAccess dbAccess,
                                 AnyValue[] params,
                                 ExpressionCursors cursors,
                                 AnyValue[] expressionVariables );

    /**
     * Gets an already projected grouping key from the context.
     */
    AnyValue getGroupingKey( CypherRow context );
}
