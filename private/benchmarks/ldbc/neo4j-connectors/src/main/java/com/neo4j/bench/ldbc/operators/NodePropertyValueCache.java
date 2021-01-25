/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public interface NodePropertyValueCache<T>
{
    interface NodePropertyValueCacheFactory
    {
        <T> NodePropertyValueCache<T> create( String key );
    }

    class NodePropertyValueCacheFactoryImpl implements NodePropertyValueCacheFactory
    {
        @Override
        public <T> NodePropertyValueCache<T> create( String key )
        {
            return new NodePropertyValueCacheImpl<>( key );
        }
    }

    T value( Node node );

    T value( long nodeId, Transaction transaction );

    class NodePropertyValueCacheImpl<T> implements NodePropertyValueCache<T>
    {
        private final Long2ObjectMap<T> values;
        private final String key;

        public NodePropertyValueCacheImpl( String key )
        {
            this.values = new Long2ObjectOpenHashMap<>();
            this.key = key;
        }

        @Override
        public T value( Node node )
        {
            if ( values.containsKey( node.getId() ) )
            {
                return values.get( node.getId() );
            }
            else
            {
                T value = (T) node.getProperty( key );
                values.put( node.getId(), value );
                return value;
            }
        }

        @Override
        public T value( long nodeId, Transaction transaction )
        {
            if ( values.containsKey( nodeId ) )
            {
                return values.get( nodeId );
            }
            else
            {
                Node node = transaction.getNodeById( nodeId );
                T value = (T) node.getProperty( key );
                values.put( node.getId(), value );
                return value;
            }
        }
    }
}
