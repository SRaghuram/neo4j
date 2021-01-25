/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public interface NodePropertiesValueCache
{
    interface NodePropertiesValueCacheFactory
    {
        NodePropertiesValueCache create( String... keys );
    }

    class NodePropertiesValueCacheFactoryImpl implements NodePropertiesValueCacheFactory
    {
        @Override
        public NodePropertiesValueCache create( String... keys )
        {
            return new NodePropertiesValueCacheImpl( keys );
        }
    }

    Map<String,Object> values( Node node );

    Map<String,Object> values( long nodeId, Transaction transaction );

    class NodePropertiesValueCacheImpl implements NodePropertiesValueCache
    {
        private final Long2ObjectMap<Map<String,Object>> values;
        private final String[] keys;

        public NodePropertiesValueCacheImpl( String... keys )
        {
            this.values = new Long2ObjectOpenHashMap<>();
            this.keys = keys;
        }

        @Override
        public Map<String,Object> values( Node node )
        {
            if ( values.containsKey( node.getId() ) )
            {
                return values.get( node.getId() );
            }
            else
            {
                Map<String,Object> values = node.getProperties( keys );
                this.values.put( node.getId(), values );
                return values;
            }
        }

        @Override
        public Map<String,Object> values( long nodeId, Transaction transaction )
        {
            if ( values.containsKey( nodeId ) )
            {
                return values.get( nodeId );
            }
            else
            {
                Node node = transaction.getNodeById( nodeId );
                Map<String,Object> value = node.getProperties( keys );
                values.put( node.getId(), value );
                return value;
            }
        }
    }
}
