/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

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
     * @return an evaluated result from the compiled expression and given input.
     */
    AnyValue evaluate( ExecutionContext context, DbAccess dbAccess, MapValue params, ExpressionCursors cursors );
}
