/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExecutionContext;
import org.neo4j.cypher.internal.runtime.KernelAPISupport$;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

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

    private static ValueGroup[] RANGE_SEEKABLE_VALUE_GROUPS = KernelAPISupport$.MODULE$.RANGE_SEEKABLE_VALUE_GROUPS();

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
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

    public static boolean possibleRangePredicate( IndexQuery query )
    {
        ValueGroup valueGroup = query.valueGroup();

        for ( ValueGroup rangeSeekableValueGroup : RANGE_SEEKABLE_VALUE_GROUPS )
        {
            if ( valueGroup == rangeSeekableValueGroup )
            {
                return true;
            }
        }
        return false;
    }
}
