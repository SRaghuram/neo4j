/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.operators;

import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.ldbc.driver.DbException;
import com.ldbc.driver.util.Function2;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public interface ManyToManyExpandCache
{
    interface ManyToManyExpandCacheFactory
    {
        ManyToManyExpandCache lru(
                int maxCacheSize,
                Direction direction,
                RelationshipType... relationshipTypes
        ) throws DbException;

        ManyToManyExpandCache lazyPullAnyPredicate(
                Predicate<Node> neighborPredicate,
                Direction direction,
                RelationshipType... relationshipTypes
        ) throws DbException;
    }

    class ManyToManyExpandCacheFactoryImpl implements ManyToManyExpandCacheFactory
    {
        @Override
        public ManyToManyExpandCache lru(
                int maxCacheSize,
                Direction direction,
                RelationshipType... relationshipTypes
        ) throws DbException
        {
            return new LruManyToManyExpandCache( maxCacheSize, direction, relationshipTypes );
        }

        @Override
        public ManyToManyExpandCache lazyPullAnyPredicate(
                Predicate<Node> neighborPredicate,
                Direction direction,
                RelationshipType... relationshipTypes
        ) throws DbException
        {
            return new LazyPullAnyPredicateManyToManyExpandCache( neighborPredicate, direction, relationshipTypes );
        }
    }

    LongSet expandIds( Node thing ) throws DbException;

    class LruManyToManyExpandCache implements ManyToManyExpandCache
    {
        private final Cache<Long,LongSet> lruCache;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final RelationshipType[] relationshipTypes;
        private final Direction direction;

        public LruManyToManyExpandCache(
                int maxCacheSize,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            this.lruCache = CacheBuilder.newBuilder()
                    .concurrencyLevel( 1 )
                    .maximumSize( maxCacheSize )
                    .build();
            this.neighborFun = Operators.neighborFun( direction );
            this.relationshipTypes = relationshipTypes;
            this.direction = direction;
        }

        @Override
        public LongSet expandIds( Node thing ) throws DbException
        {
            LongSet neighborIds = lruCache.getIfPresent( thing.getId() );
            if ( null == neighborIds )
            {
                neighborIds = Operators.expandIds( thing, neighborFun, direction, relationshipTypes );
                lruCache.put( thing.getId(), neighborIds );
            }
            return neighborIds;
        }
    }

    // TODO is actually different cache type with different behavior, should not be here
    // TODO ManyToManyPredicateExpandCache <-- perhaps create that interface instead
    class LazyPullAnyPredicateManyToManyExpandCache implements ManyToManyExpandCache
    {
        private final Long2ObjectMap<LongSet> cache;
        private final Function2<Relationship,Node,Node,DbException> neighborFun;
        private final RelationshipType[] relationshipTypes;
        private final Direction direction;
        private final Predicate<Node> neighborPredicate;

        public LazyPullAnyPredicateManyToManyExpandCache(
                Predicate<Node> neighborPredicate,
                Direction direction,
                RelationshipType... relationshipTypes ) throws DbException
        {
            this.neighborPredicate = neighborPredicate;
            this.direction = direction;
            this.relationshipTypes = relationshipTypes;
            this.cache = new Long2ObjectOpenHashMap<>();
            this.neighborFun = Operators.neighborFun( direction );
        }

        @Override
        public LongSet expandIds( Node thing ) throws DbException
        {
            long thingId = thing.getId();
            LongSet neighborIds = cache.get( thingId );
            if ( null == neighborIds )
            {
                neighborIds = Operators.expandIdsIf(
                        neighborPredicate,
                        thing,
                        neighborFun,
                        direction,
                        relationshipTypes );
                cache.put( thingId, neighborIds );
            }
            return neighborIds;
        }
    }
}
