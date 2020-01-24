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

import java.io.IOException;
import java.util.stream.Stream;

import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class MainStores extends LifecycleAdapter
{
    final LifeSupport life = new LifeSupport();
    final SimpleStore mainStore;
    final SimpleStore mainLargeStore;
    private final SimpleStore[] mainStores;
    final SimpleBigValueStore bigPropertyValueStore;

    MainStores( SimpleStore mainStore, SimpleStore mainLargeStore, SimpleBigValueStore bigPropertyValueStore )
    {
        this.mainStore = mainStore;
        this.mainLargeStore = mainLargeStore;
        this.bigPropertyValueStore = bigPropertyValueStore;
        this.mainStores = registerStoresBySizeExp( mainStore, mainLargeStore );
        life.add( mainStore );
        life.add( mainLargeStore );
        life.add( bigPropertyValueStore );
    }

    private static SimpleStore[] registerStoresBySizeExp( SimpleStore... stores )
    {
        int maxSizeExp = Stream.of( stores ).mapToInt( SimpleStore::recordSizeExponential ).max().getAsInt();
        SimpleStore[] result = new SimpleStore[maxSizeExp + 1];
        Stream.of( stores ).forEach( store -> result[store.recordSizeExponential()] = store );
        return result;
    }

    SimpleStore mainStore( int sizeExp )
    {
        return sizeExp >= mainStores.length ? null : mainStores[sizeExp];
    }

    public SimpleStore nextLargerMainStore( int sizeExp )
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

    void flushAndForce( IOLimiter limiter, PageCursorTracer cursorTracer )
    {
        mainStore.flush( cursorTracer );
        mainLargeStore.flush( cursorTracer );
        bigPropertyValueStore.flush( cursorTracer );
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }

    PageCursor[] openMainStoreWriteCursors() throws IOException
    {
        PageCursor[] cursors = new PageCursor[mainStores.length];
        for ( int i = 0; i < mainStores.length; i++ )
        {
            if ( mainStores[i] != null )
            {
                cursors[i] = mainStores[i].openWriteCursor();
            }
        }
        return cursors;
    }
}
