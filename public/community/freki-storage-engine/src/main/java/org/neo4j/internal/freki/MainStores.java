/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.freki;

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.Life;

import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.internal.helpers.ArrayUtil.concat;
import static org.neo4j.io.IOUtils.closeAllSilently;

class MainStores extends Life
{
    public final SimpleStore mainStore;
    private final SimpleStore[] mainStores;
    public final SimpleBigValueStore bigPropertyValueStore;
    public final DenseStore denseStore;
    protected final List<Pair<IdGeneratorFactory,IdType>> idGeneratorsToRegisterOnTheWorkSync = new ArrayList<>();

    MainStores( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, IdGeneratorFactory idGeneratorFactory,
            PageCacheTracer pageCacheTracer, PageCursorTracerSupplier cursorTracerSupplier, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean createStoreIfNotExists ) throws IOException
    {
        SimpleStore[] mainStores = new SimpleStore[4];
        BigPropertyValueStore bigPropertyValueStore = null;
        DenseStore denseStore = null;
        boolean success = false;
        try
        {
            if ( createStoreIfNotExists )
            {
                fs.mkdirs( databaseLayout.databaseDirectory() );
            }
            mainStores[0] = new Store( databaseLayout.file( "main-store-x1" ), pageCache, idGeneratorFactory, IdType.NODE, false, createStoreIfNotExists, 0,
                    cursorTracerSupplier );
            idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( idGeneratorFactory, IdType.NODE ) );
            for ( int i = 1; i < mainStores.length; i++ )
            {
                IdGeneratorFactory separateIdGeneratorFactory = new DefaultIdGeneratorFactory( fs, recoveryCleanupWorkCollector, false );
                mainStores[i] = new Store( databaseLayout.file( "main-store-x" + recordXFactor( i ) ), pageCache, separateIdGeneratorFactory,
                        IdType.NODE, false, createStoreIfNotExists, i, cursorTracerSupplier );
                idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( separateIdGeneratorFactory, IdType.NODE ) );
            }
            bigPropertyValueStore =
                    new BigPropertyValueStore( databaseLayout.file( "big-values" ), pageCache, false, createStoreIfNotExists, cursorTracerSupplier );
            denseStore = new DenseStore( pageCache, databaseLayout.file( "dense-store" ), recoveryCleanupWorkCollector, false, pageCacheTracer,
                    bigPropertyValueStore );
            success = true;

            this.mainStores = mainStores;
            this.mainStore = mainStores[0];
            this.bigPropertyValueStore = bigPropertyValueStore;
            this.denseStore = denseStore;
            addMainStoresToLife();
        }
        finally
        {
            if ( !success )
            {
                closeAllSilently( concat( mainStores, bigPropertyValueStore, denseStore ) );
            }
        }
    }

    MainStores( SimpleStore[] mainStores, SimpleBigValueStore bigPropertyValueStore, DenseStore denseStore )
    {
        this.mainStores = mainStores;
        this.mainStore = mainStores[0];
        this.bigPropertyValueStore = bigPropertyValueStore;
        this.denseStore = denseStore;
        addMainStoresToLife();
    }

    private void addMainStoresToLife()
    {
        for ( SimpleStore store : mainStores )
        {
            if ( store != null )
            {
                life.add( store );
            }
        }
        life.add( bigPropertyValueStore );
        // TODO: 2020-02-05 for now just check for null, later on extract interface and inject test-store in tests
        if ( denseStore != null )
        {
            life.add( denseStore );
        }
    }

    void idGenerators( Consumer<IdGenerator> visitor )
    {
        idGeneratorsToRegisterOnTheWorkSync.forEach( idGenerator -> visitor.accept( idGenerator.getKey().get( idGenerator.getValue() ) ) );
    }

    SimpleStore mainStore( int sizeExp )
    {
        return sizeExp >= mainStores.length ? null : mainStores[sizeExp];
    }

    SimpleStore nextLargerMainStore( int sizeExp )
    {
        for ( int i = sizeExp + 1; i < mainStores.length; i++ )
        {
            if ( mainStores[i] != null )
            {
                return mainStores[i];
            }
        }
        return null;
    }

    SimpleStore largestMainStore()
    {
        for ( int i = mainStores.length - 1; i >= 0; i-- )
        {
            if ( mainStores[i] != null )
            {
                return mainStores[i];
            }
        }
        throw new IllegalStateException( "No stores" );
    }

    SimpleStore storeSuitableForRecordSize( int recordSize, int atLeastSizeExp )
    {
        SimpleStore candidate = mainStore;
        while ( candidate != null )
        {
            if ( candidate.recordSizeExponential() >= atLeastSizeExp && recordSize <= candidate.recordDataSize() )
            {
                return candidate;
            }
            candidate = nextLargerMainStore( candidate.recordSizeExponential() );
        }
        return null;
    }

    int getNumMainStores()
    {
        return mainStores.length;
    }

    void flushAndForce( IOLimiter limiter, PageCursorTracer cursorTracer ) throws IOException
    {
        for ( SimpleStore mainStore : mainStores )
        {
            mainStore.flush( limiter, cursorTracer );
        }
        bigPropertyValueStore.flush( limiter, cursorTracer );
        denseStore.checkpoint( limiter, cursorTracer );
    }
}
