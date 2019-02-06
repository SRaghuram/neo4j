/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.tempindex;

import java.util.HashMap;
import java.util.Map;

public class HashMapTempIndexFactory implements TempIndexFactory
{
    @Override
    public TempIndex create()
    {
        return new HashMapTempIndex();
    }

    public static class HashMapTempIndex implements TempIndex
    {
        private Map<Long,Long> map = new HashMap<>();

        @Override
        public void put( long k, long v )
        {
            map.put( k, v );
        }

        @Override
        public long get( long k )
        {
            return map.get( k );
        }

        @Override
        public void shutdown()
        {
            map = null;
        }
    }
}
