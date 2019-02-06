/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import java.util.HashMap;
import java.util.Map;

public class TestUtils
{
    public static <T> Map<String,T> map( String key, T value, Object... keyValues )
    {
        assert keyValues.length % 2 == 0;
        Map<String,T> theMap = new HashMap<>();
        theMap.put( key, value );
        for ( int i = 0; i < keyValues.length; i += 2 )
        {
            theMap.put( (String) keyValues[i], (T) keyValues[i + 1] );
        }
        return theMap;
    }
}
