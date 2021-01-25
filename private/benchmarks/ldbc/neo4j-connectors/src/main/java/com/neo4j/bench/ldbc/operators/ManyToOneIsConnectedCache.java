/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public interface ManyToOneIsConnectedCache
{
    interface ManyToOneIsConnectedCacheFactory
    {
        ManyToOneIsConnectedCache lazyPull(
                LongSet thingIds,
                RelationshipType relationshipType,
                Direction direction );

        ManyToOneIsConnectedCache lazyPush(
                Node baseThing,
                RelationshipType relationshipType,
                Direction direction ) throws DbException;

        ManyToOneIsConnectedCache nestedLazyPull(
                ManyToOneIsConnectedCache innerManyToOneIsConnectedCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException;

        ManyToOneIsConnectedCache nestedNonCaching(
                ManyToOneIsConnectedCache innerManyToOneIsConnectedCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException;
    }

    class ManyToOneIsConnectedCacheFactoryImpl implements ManyToOneIsConnectedCacheFactory
    {
        @Override
        public ManyToOneIsConnectedCache lazyPull(
                LongSet thingIds,
                RelationshipType relationshipType,
                Direction direction )
        {
            return new LazyPullManyToOneIsConnectedCache( thingIds, relationshipType, direction );
        }

        @Override
        public ManyToOneIsConnectedCache lazyPush(
                Node baseThing,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            return new LazyPushManyToOneIsConnectedCache( baseThing, relationshipType, direction );
        }

        @Override
        public ManyToOneIsConnectedCache nestedLazyPull(
                ManyToOneIsConnectedCache innerCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            return new NestedLazyPullManyToOneIsConnectedCache( innerCache, relationshipType, direction );
        }

        @Override
        public ManyToOneIsConnectedCache nestedNonCaching(
                ManyToOneIsConnectedCache innerCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            return new NestedNonCachingManyToOneIsConnectedCache( innerCache, relationshipType, direction );
        }
    }

    boolean isConnected( Node thing ) throws DbException;

    class LazyPullManyToOneIsConnectedCache implements ManyToOneIsConnectedCache
    {
        private final LongSet prePopulatedThingIds;
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final LongSet thingsConnected;
        private final LongSet thingsNotConnected;

        public LazyPullManyToOneIsConnectedCache(
                LongSet prePopulatedThingIds,
                RelationshipType relationshipType,
                Direction direction )
        {
            this.prePopulatedThingIds = prePopulatedThingIds;
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.neighborFun = Operators.neighborFun( direction );
            this.thingsConnected = new LongOpenHashSet();
            this.thingsNotConnected = new LongOpenHashSet();
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            long thingId = thing.getId();
            if ( thingsConnected.contains( thingId ) )
            {
                return true;
            }
            else if ( thingsNotConnected.contains( thingId ) )
            {
                return false;
            }
            else
            {
                Node neighboringThing = neighborFun.apply(
                        thing.getSingleRelationship( relationshipType, direction ),
                        thing
                );
                if ( prePopulatedThingIds.contains( neighboringThing.getId() ) )
                {
                    thingsConnected.add( thingId );
                    return true;
                }
                else
                {
                    thingsNotConnected.add( thingId );
                    return false;
                }
            }
        }
    }

    class LazyPushManyToOneIsConnectedCache implements ManyToOneIsConnectedCache
    {
        private final LongSet thingsConnected;
        private final Iterator<Relationship> connectionsIterator;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final Node baseThing;

        public LazyPushManyToOneIsConnectedCache(
                Node baseThing,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.baseThing = baseThing;
            this.thingsConnected = new LongOpenHashSet();
            this.connectionsIterator = baseThing.getRelationships( direction, relationshipType ).iterator();
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            if ( thingsConnected.contains( thing.getId() ) )
            {
                return true;
            }
            while ( connectionsIterator.hasNext() )
            {
                Node neighborThing = neighborFun.apply( connectionsIterator.next(), baseThing );
                thingsConnected.add( neighborThing.getId() );
                if ( thing.equals( neighborThing ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    class NestedLazyPullManyToOneIsConnectedCache implements ManyToOneIsConnectedCache
    {
        private final ManyToOneIsConnectedCache innerManyToOneIsConnectedCache;
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final LongSet thingsConnected;
        private final LongSet thingsNotConnected;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;

        public NestedLazyPullManyToOneIsConnectedCache(
                ManyToOneIsConnectedCache innerManyToOneIsConnectedCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.innerManyToOneIsConnectedCache = innerManyToOneIsConnectedCache;
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.thingsConnected = new LongOpenHashSet();
            this.thingsNotConnected = new LongOpenHashSet();
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            long thingId = thing.getId();
            if ( thingsConnected.contains( thingId ) )
            {
                return true;
            }
            else if ( thingsNotConnected.contains( thingId ) )
            {
                return false;
            }
            else
            {
                Node neighboringThing = neighborFun.apply(
                        thing.getSingleRelationship( relationshipType, direction ),
                        thing
                );
                if ( innerManyToOneIsConnectedCache.isConnected( neighboringThing ) )
                {
                    thingsConnected.add( thingId );
                    return true;
                }
                else
                {
                    thingsNotConnected.add( thingId );
                    return false;
                }
            }
        }
    }

    class NestedNonCachingManyToOneIsConnectedCache implements ManyToOneIsConnectedCache
    {
        private final ManyToOneIsConnectedCache innerManyToOneIsConnectedCache;
        private final RelationshipType relationshipType;
        private final Direction direction;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;

        public NestedNonCachingManyToOneIsConnectedCache(
                ManyToOneIsConnectedCache innerManyToOneIsConnectedCache,
                RelationshipType relationshipType,
                Direction direction ) throws DbException
        {
            this.innerManyToOneIsConnectedCache = innerManyToOneIsConnectedCache;
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public boolean isConnected( Node thing ) throws DbException
        {
            Node neighboringThing = neighborFun.apply(
                    thing.getSingleRelationship( relationshipType, direction ),
                    thing
            );
            return innerManyToOneIsConnectedCache.isConnected( neighboringThing );
        }
    }
}
