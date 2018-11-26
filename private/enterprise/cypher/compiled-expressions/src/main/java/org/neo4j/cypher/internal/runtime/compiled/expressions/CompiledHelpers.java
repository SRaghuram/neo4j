/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.v4_0.util.CypherTypeException;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Contains helper methods used from compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CompiledHelpers
{
    private CompiledHelpers()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue ) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    public static AnyValue loadVariable( ExecutionContext ctx, String name )
    {
        if ( !ctx.contains( name ) )
        {
            throw new NotFoundException( String.format( "Unknown variable `%s`.", name ) );
        }
        return ctx.apply( name );
    }

    public static Value cachedProperty( ExecutionContext ctx, DbAccess dbAccess, int nodeOffset, int propertyKey,
            int propertyOffset )
    {
        long nodeId = ctx.getLongAt( nodeOffset );
        if ( nodeId == StatementConstants.NO_SUCH_NODE || propertyKey == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return NO_VALUE;
        }
        else
        {
            Value propertyOrNull = dbAccess.getTxStateNodePropertyOrNull( nodeId, propertyKey );
            if ( propertyOrNull == null )
            {
                return ctx.getCachedPropertyAt( propertyOffset );
            }
            return propertyOrNull;
        }
    }

    public static AnyValue nodeOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long nodeId = context.getLongAt( offset );
        return nodeId == -1 ? NO_VALUE : dbAccess.nodeById( nodeId );
    }

    public static AnyValue relationshipOrNoValue( ExecutionContext context, DbAccess dbAccess, int offset )
    {
        long relationshipId = context.getLongAt( offset );
        return relationshipId == -1 ? NO_VALUE : dbAccess.relationshipById( relationshipId );
    }
}
