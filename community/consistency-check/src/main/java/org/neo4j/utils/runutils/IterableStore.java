/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.utils.runutils;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.propertystorereorg.NodeIdMapByLabel;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreIdIterator;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapLongArray;

import static org.neo4j.kernel.impl.store.RecordStore.IN_USE;

public class IterableStore<RECORD extends AbstractBaseRecord> implements BoundedIterable<RECORD>
{
    private final RecordStore<RECORD> store;
    private boolean forward = true;
    private NodeIdMapByLabel useNodeIdMap = null;
    private StoreAccess storeAccess;

    public IterableStore( RecordStore<RECORD> store, StoreAccess storeAccess )
    {
        this.store = store;
        this.storeAccess = storeAccess;
    }

    @Override
    public long maxCount()
    {
        return store.getHighId();
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public Iterator<RECORD> iterator()
    {
        return Scanner.scan( store, storeAccess, forward, useNodeIdMap, IN_USE ).iterator();
    }
    
    public void setDirection(boolean forward )
    {
    	this.forward = forward;
    }
    
    public void setUseNodeIdMap( NodeIdMapByLabel useNodeIdMap)
    {
        this.useNodeIdMap = useNodeIdMap;
    }
    
    
    public static class Scanner
    {
        @SafeVarargs
        public static <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore<R> store,
                final Predicate<? super R>... filters )
        {
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store );

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan:
                            while ( ids.hasNext() )
                            {
                                R record = store.forceGetRecord( ids.next() );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) )
                                    {
                                        continue scan;
                                    }
                                }
                                return record;
                            }
                            return null;
                        }
                    };
                }
            };
        }
        
        @SafeVarargs
        public static <R extends AbstractBaseRecord> Iterable<R> scan( final RecordStore<R> store,
                StoreAccess access, final boolean forward, NodeIdMapByLabel useMap, final Predicate<? super R>... filters )
        {
            final StoreAccess storeAccess = access;
            final NodeIdMapByLabel useNodeIdMap = useMap;
            return new Iterable<R>()
            {
                @Override
                public Iterator<R> iterator()
                {
                    return new PrefetchingIterator<R>()
                    {
                        final PrimitiveLongIterator ids = new StoreIdIterator( store , forward);

                        @Override
                        protected R fetchNextOrNull()
                        {
                            scan:
                            while ( ids.hasNext() )
                            {
                                long idNext = useNodeIdMap == null ? ids.next() : useNodeIdMap.get (ids.next());
                                R record = storeAccess.getRecord( store, idNext );
                                for ( Predicate<? super R> filter : filters )
                                {
                                    if ( !filter.accept( record ) )
                                    {
                                        continue scan;
                                    }
                                }
                                return record;
                            }
                            return null;
                        }
                    };
                }
            };
        }

        public static <R extends AbstractBaseRecord> Iterable<R> scanById( final RecordStore<R> store,
                Iterable<Long> ids )
        {
            return new IterableWrapper<R,Long>( ids )
            {
                @Override
                protected R underlyingObjectToObject( Long id )
                {
                    return store.forceGetRecord( id );
                }
            };
        }
    }
}
