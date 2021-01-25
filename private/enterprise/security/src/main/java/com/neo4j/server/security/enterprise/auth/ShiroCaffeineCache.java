/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;

public class ShiroCaffeineCache<K, V> implements Cache<K,V>
{
    private final com.github.benmanes.caffeine.cache.Cache<K,V> caffCache;

    ShiroCaffeineCache( Ticker ticker, CaffeineCacheFactory cacheFactory, long ttl, int maxCapacity, boolean useTTL )
    {
        if ( useTTL )
        {
            if ( ttl <= 0 )
            {
                throw new IllegalArgumentException( "TTL must be larger than zero." );
            }
            caffCache = cacheFactory.createCache( ticker, ttl, maxCapacity );
        }
        else
        {
            caffCache = cacheFactory.createCache( maxCapacity );
        }
    }

    @Override
    public V get( K key ) throws CacheException
    {
        return caffCache.getIfPresent( key );
    }

    @Override
    public V put( K key, V value ) throws CacheException
    {
        return caffCache.asMap().put( key, value );
    }

    @Override
    public V remove( K key ) throws CacheException
    {
        return caffCache.asMap().remove( key );
    }

    @Override
    public void clear() throws CacheException
    {
        caffCache.invalidateAll();
    }

    @Override
    public int size()
    {
        return caffCache.asMap().size();
    }

    @Override
    public Set<K> keys()
    {
        return caffCache.asMap().keySet();
    }

    @Override
    public Collection<V> values()
    {
        return caffCache.asMap().values();
    }

    public static class Manager implements CacheManager
    {
        private final Map<String,Cache<?,?>> caches;
        private final Ticker ticker;
        private final long ttl;
        private CaffeineCacheFactory cacheFactory;
        private final int maxCapacity;
        private boolean useTTL;

        public Manager( Ticker ticker, long ttl, CaffeineCacheFactory cacheFactory, int maxCapacity, boolean useTTL )
        {
            this.ticker = ticker;
            this.ttl = ttl;
            this.cacheFactory = cacheFactory;
            this.maxCapacity = maxCapacity;
            this.useTTL = useTTL;
            caches = new HashMap<>();
        }

        @Override
        public <K, V> Cache<K,V> getCache( String s ) throws CacheException
        {
            //noinspection unchecked
            return (Cache<K,V>) caches.computeIfAbsent( s,
                    ignored -> useTTL && ttl <= 0 ? new NullCache() : new ShiroCaffeineCache<K,V>( ticker, cacheFactory, ttl, maxCapacity, useTTL ) );
        }
    }

    private static class NullCache<K, V> implements Cache<K, V>
    {
        @Override
        public V get( K key ) throws CacheException
        {
            return null;
        }

        @Override
        public V put( K key, V value ) throws CacheException
        {
            return null;
        }

        @Override
        public V remove( K key ) throws CacheException
        {
            return null;
        }

        @Override
        public void clear() throws CacheException
        {

        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public Set<K> keys()
        {
            return Collections.emptySet();
        }

        @Override
        public Collection<V> values()
        {
            return Collections.emptySet();
        }
    }
}
