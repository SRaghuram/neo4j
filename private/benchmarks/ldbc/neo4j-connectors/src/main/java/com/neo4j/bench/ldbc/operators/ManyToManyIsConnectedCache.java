/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

// TODO needs optimizations to account for case when BOTH direction & same Label no both sides
// TODO e.g., with Person KNOWS Person either side can be checked for connectedness, and only one may already be cached
public interface ManyToManyIsConnectedCache
{
    interface ManyToManyIsConnectedCacheFactory
    {
        ManyToManyIsConnectedCache lazyPull(
                RelationshipType relationshipType,
                Direction direction ) throws DbException;
    }

    class ManyToOneExpandCacheFactoryImpl implements ManyToManyIsConnectedCacheFactory
    {
        @Override
        public ManyToManyIsConnectedCache lazyPull( RelationshipType relationshipType, Direction direction )
                throws DbException
        {
            return new LazyPullManyToManyIsConnectedCache( relationshipType, direction );
        }
    }

    boolean areConnected( Node fromThing, Node toThing ) throws DbException;

    class LazyPullManyToManyIsConnectedCache implements ManyToManyIsConnectedCache
    {
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Long2ObjectMap<LongSet> connectedMatrix;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;

        public LazyPullManyToManyIsConnectedCache(
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.connectedMatrix = new Long2ObjectOpenHashMap<>();
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public boolean areConnected( Node fromThing, Node toThing ) throws DbException
        {
            if ( !connectedMatrix.containsKey( fromThing.getId() ) )
            {
                LongSet fromThingConnections = new LongOpenHashSet();
                for ( Relationship relationship : fromThing.getRelationships( direction, relationshipType ) )
                {
                    fromThingConnections.add( neighborFun.apply( relationship, fromThing ).getId() );
                }
                connectedMatrix.put( fromThing.getId(), fromThingConnections );
            }
            return connectedMatrix.get( fromThing.getId() ).contains( toThing.getId() );
        }
    }
}
