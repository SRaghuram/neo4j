/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.operators;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

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

    Map<String,Object> values( long nodeId, GraphDatabaseService db );

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
        public Map<String,Object> values( long nodeId, GraphDatabaseService db )
        {
            if ( values.containsKey( nodeId ) )
            {
                return values.get( nodeId );
            }
            else
            {
                Node node = db.getNodeById( nodeId );
                Map<String,Object> value = node.getProperties( keys );
                values.put( node.getId(), value );
                return value;
            }
        }
    }
}
