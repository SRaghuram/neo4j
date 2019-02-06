/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer.tempindex;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class OffHeapTempIndexFactory implements TempIndexFactory
{
    @Override
    public TempIndex create()
    {
        return new OffHeapTempIndex();
    }

    public static class OffHeapTempIndex implements TempIndex
    {
        private final Long2LongMap map;
        private boolean shutdown;

        public OffHeapTempIndex()
        {
            map = new Long2LongOpenHashMap( 2 ^ 16 );
        }

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
        public synchronized void shutdown()
        {
            if ( !shutdown )
            {
                shutdown = true;
            }
        }
    }
}
