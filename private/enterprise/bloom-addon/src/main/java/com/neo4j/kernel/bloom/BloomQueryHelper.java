/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.bloom;

import org.apache.lucene.queryparser.classic.QueryParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class BloomQueryHelper
{
    public static String createQuery( Collection<String> query, boolean fuzzy, boolean matchAll )
    {
        query = query.stream().flatMap( s -> Arrays.stream( s.split( "\\s+" ) ) ).collect( Collectors.toList() );

        String delim = "";
        if ( matchAll )
        {
            delim = "&& ";
        }
        if ( fuzzy )
        {
            return query.stream().map( QueryParser::escape ).collect( joining( "~ " + delim, "", "~" ) );
        }
        return query.stream().map( QueryParser::escape ).collect( joining( " " + delim ) );
    }
}
