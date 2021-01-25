/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.cypher.internal.runtime.ReadableRow;
import org.neo4j.values.AnyValue;

/**
 * Interface implemented by compiled expressions.
 */
public interface CompiledExpression
{
    /**
     * Evaluates the result of an expression
     *
     * @param context the current context.
     * @param dbAccess used for accessing the database
     * @param params the parameters of the query
     * @param cursors cursors to use for expression evaluation
     * @param expressionVariables array used for storing expression variable values
     * @return an evaluated result from the compiled expression and given input.
     */
    AnyValue evaluate( ReadableRow context,
                       DbAccess dbAccess,
                       AnyValue[] params,
                       ExpressionCursors cursors,
                       AnyValue[] expressionVariables );
}
