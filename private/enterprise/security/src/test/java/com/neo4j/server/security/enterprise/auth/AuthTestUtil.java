/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class AuthTestUtil
{
    private AuthTestUtil()
    {
    }

    public static <T> T[] with( Class<T> clazz, T[] things, T... moreThings )
    {
        return Stream.concat( Arrays.stream(things), Arrays.stream( moreThings ) ).toArray(
                size -> (T[]) Array.newInstance( clazz, size )
        );
    }

    public static String[] with( String[] things, String... moreThings )
    {
        return Stream.concat( Arrays.stream(things), Arrays.stream( moreThings ) ).toArray( String[]::new );
    }

    public static <T> List<T> listOf( T... values )
    {
        return Stream.of( values ).collect( Collectors.toList() );
    }

}
