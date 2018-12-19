/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
 * Interface implemented by compiled projection.
 */
public interface CompiledGroupingExpression
{
    /**
     * Projects the given grouping key to the given context
     * @param context the context to write to
     * @param groupingKey the grouping key to project
     */
    void projectGroupingKey( ExecutionContext context, AnyValue groupingKey );

    /**
     * Computes grouping key.
     */
    AnyValue computeGroupingKey( ExecutionContext context, DbAccess dbAccess, MapValue params, ExpressionCursors cursors );

    /**
     * Gets an already projected grouping key from the context.
     */
    AnyValue getGroupingKey( ExecutionContext context );
}
