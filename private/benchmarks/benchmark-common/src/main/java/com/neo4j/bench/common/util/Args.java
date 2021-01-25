/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Args
{
    public static String[] concatArgs( String[] leftArgs, String[] rightArgs )
    {
        String[] allJvmArgsArray = new String[leftArgs.length + rightArgs.length];
        System.arraycopy( leftArgs, 0, allJvmArgsArray, 0, leftArgs.length );
        System.arraycopy( rightArgs, 0, allJvmArgsArray, leftArgs.length, rightArgs.length );
        return allJvmArgsArray;
    }

    public static List<String> concatArgs( List<String> left, List<String> right )
    {
        List<String> concat = new ArrayList<>();
        concat.addAll( left );
        concat.addAll( right );
        return concat;
    }

    public static String[] splitArgs( String string, String sep )
    {
        return Stream.of( string.split( sep ) )
                     .map( String::trim )
                     .filter( s -> !s.isEmpty() )
                     .toArray( String[]::new );
    }

    public static List<String> splitArgs( String argsString )
    {
        return Stream.of( argsString.split( " " ) )
                     .map( String::trim )
                     .filter( s -> !s.isEmpty() )
                     .collect( toList() );
    }
}
