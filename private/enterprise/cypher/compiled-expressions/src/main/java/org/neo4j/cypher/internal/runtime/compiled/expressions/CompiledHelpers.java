/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.neo4j.cypher.internal.v3_5.util.CypherTypeException;

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext;
import org.neo4j.graphdb.NotFoundException;
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
}
