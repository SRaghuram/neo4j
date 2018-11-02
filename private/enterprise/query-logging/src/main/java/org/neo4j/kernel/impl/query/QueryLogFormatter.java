/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.neo4j.helpers.Strings;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.values.AnyValue;
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
        Long bytes = query.allocatedBytes();
        if ( bytes != null )
        {
            result.append( bytes ).append( " B - " );
        }
    }

    static void formatDetailedTime( StringBuilder result, QuerySnapshot query )
    {
        result.append( "(planning: " ).append( query.compilationTimeMillis() );
        Long cpuTime = query.cpuTimeMillis();
        if ( cpuTime != null )
        {
            result.append( ", cpu: " ).append( cpuTime );
        }
        result.append( ", waiting: " ).append( query.waitTimeMillis() );
        result.append( ") - " );
    }

    static void formatMapValue( StringBuilder result, MapValue params, Collection<String> obfuscate )
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

                if ( obfuscate.contains( key ) )
                {
                    result.append( "******" );
                }
                else
                {
                    result.append( formatAnyValue( value ) );
                }
                sep[0] = ", ";
            } );
        }
        result.append( "}" );
    }

    static String formatAnyValue( AnyValue value )
    {
        PrettyPrinter printer = new PrettyPrinter( "'" );
        value.writeTo( printer );
        return printer.value();
    }

    static void formatMap( StringBuilder result, Map<String,Object> params )
    {
        formatMap( result, params, Collections.emptySet() );
    }

    static void formatMap( StringBuilder result, Map<String, Object> params, Collection<String> obfuscate )
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
