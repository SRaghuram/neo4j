/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.helpers.Strings;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter.EntityMode;
import org.neo4j.values.utils.PrettyPrinter;
import org.neo4j.values.virtual.MapValue;

class QueryLogFormatter
{
    private QueryLogFormatter()
    {
    }

    static void formatPageDetails( StringBuilder result, QuerySnapshot query )
    {
        result.append( query.pageHits() ).append( " page hits, " );
        result.append( query.pageFaults() ).append( " page faults - " );
    }

    static void formatAllocatedBytes( StringBuilder result, QuerySnapshot query )
    {
        long bytes = query.allocatedBytes();
        if ( bytes != OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED )
        {
            result.append( bytes ).append( " B - " );
        }
    }

    static void formatDetailedTime( StringBuilder result, QuerySnapshot query )
    {
        result.append( "(planning: " ).append( TimeUnit.MICROSECONDS.toMillis( query.compilationTimeMicros() ) );
        Long cpuTime = query.cpuTimeMicros();
        if ( cpuTime != null )
        {
            result.append( ", cpu: " ).append( TimeUnit.MICROSECONDS.toMillis( cpuTime ) );
        }
        result.append( ", waiting: " ).append( TimeUnit.MICROSECONDS.toMillis( query.waitTimeMicros() ) );
        result.append( ") - " );
    }

    static void formatMapValue( StringBuilder result, MapValue params, EntityMode entityMode )
    {
        result.append( '{' );
        if ( params != null )
        {
            final String[] sep = new String[]{""};
            params.foreach( ( key, value ) -> {
                result
                        .append( sep[0] )
                        .append( key )
                        .append( ": " );

                formatAnyValue( value, result, entityMode );
                sep[0] = ", ";
            } );
        }
        result.append( "}" );
    }

    private static void formatAnyValue( AnyValue value, StringBuilder builder, EntityMode entityMode )
    {
        PrettyPrinter printer = new PrettyPrinter( "'", entityMode );
        value.writeTo( printer );
        printer.valueInto( builder );
    }

    static void formatMap( StringBuilder result, Map<String,Object> params )
    {
        formatMap( result, params, Collections.emptySet() );
    }

    private static void formatMap( StringBuilder result, Map<String,Object> params, Collection<String> obfuscate )
    {
        result.append( '{' );
        if ( params != null )
        {
            String sep = "";
            for ( Map.Entry<String,Object> entry : params.entrySet() )
            {
                result
                        .append( sep )
                        .append( entry.getKey() )
                        .append( ": " );

                if ( obfuscate.contains( entry.getKey() ) )
                {
                    result.append( "******" );
                }
                else
                {
                    formatValue( result, entry.getValue() );
                }
                sep = ", ";
            }
        }
        result.append( "}" );
    }

    private static void formatValue( StringBuilder result, Object value )
    {
        if ( value instanceof Map<?,?> )
        {
            //noinspection unchecked
            formatMap( result, (Map<String, Object>) value, Collections.emptySet() );
        }
        else if ( value instanceof String )
        {
            result.append( '\'' ).append( value ).append( '\'' );
        }
        else
        {
            result.append( Strings.prettyPrint( value ) );
        }
    }
}
