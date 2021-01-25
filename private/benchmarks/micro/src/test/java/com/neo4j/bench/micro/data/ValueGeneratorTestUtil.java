/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

public class ValueGeneratorTestUtil
{
    static List<?> toList( Object array )
    {
        if ( !array.getClass().isArray() )
        {
            throw new RuntimeException( "Not an array: " + array.getClass().getSimpleName() );
        }
        int length = Array.getLength( array );
        List<Object> l = new ArrayList<>( length );
        for ( int i = 0; i < length; i++ )
        {
            l.add( Array.get( array, i ) );
        }
        return l;
    }
}
