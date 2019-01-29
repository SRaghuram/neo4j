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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

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

    T value( long nodeId, GraphDatabaseService db );

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
        public T value( long nodeId, GraphDatabaseService db )
        {
            if ( values.containsKey( nodeId ) )
            {
                return values.get( nodeId );
            }
            else
            {
                Node node = db.getNodeById( nodeId );
                T value = (T) node.getProperty( key );
                values.put( node.getId(), value );
                return value;
            }
        }
    }
}
