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

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

@ExtendWith( RandomExtension.class )
class GraphUpdatesTest
{
    @Inject
    private RandomRule random;

    private SimpleStore[] stores =
            new SimpleStore[]{new InMemoryTestStore( 0 ), new InMemoryTestStore( 1 ), new InMemoryTestStore( 2 ), new InMemoryTestStore( 3 )};
    private SimpleBigValueStore bigValueStore = new InMemoryBigValueTestStore();
    private MainStores mainStores = new MainStores( stores, bigValueStore, null );

    @Test
    void shouldCreateAndDeleteNodeProperties() throws ConstraintViolationTransactionFailureException
    {
        long nodeId = 0;
        {
            GraphUpdates updates = new GraphUpdates( mainStores, PageCursorTracer.NULL );
            updates.create( nodeId );
            extractAndApplyUpdates( updates );
        }

        int maxPropertyKeys = 20;
        Integer[] propertyKeys = new Integer[maxPropertyKeys];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeys[i] = i;
        }
        BitSet existing = new BitSet();
        for ( int i = 0; i < 1_000; i++ )
        {
            GraphUpdates updates = new GraphUpdates( mainStores, PageCursorTracer.NULL );
            int numChanges = random.nextInt( 1, 10 );
            GraphUpdates.NodeUpdates nodeUpdates = updates.getOrLoad( nodeId );
            List<StorageProperty> added = new ArrayList<>();
            List<StorageProperty> changed = new ArrayList<>();
            MutableIntSet removed = IntSets.mutable.empty();
            for ( int propertyKey : random.selection( propertyKeys, numChanges, numChanges, false ) )
            {
                if ( existing.get( propertyKey ) )
                {
                    if ( random.nextBoolean() )
                    {
                        changed.add( randomPropertyValue( propertyKey ) );
                    }
                    else
                    {
                        removed.add( propertyKey );
                        existing.clear( propertyKey );
                    }
                }
                else
                {
                    added.add( randomPropertyValue( propertyKey ) );
                    existing.set( propertyKey );
                }
            }
            nodeUpdates.updateNodeProperties( added, changed, removed );
            extractAndApplyUpdates( updates );
        }
    }

    private StorageProperty randomPropertyValue( int propertyKey )
    {
        return new PropertyKeyValue( propertyKey, random.nextValue() );
    }

    private void extractAndApplyUpdates( GraphUpdates updates ) throws ConstraintViolationTransactionFailureException
    {
        updates.extractUpdates( command ->
        {
            try
            {
                ((FrekiCommand) command).accept( new FrekiCommand.Dispatcher.Adapter()
                {
                    @Override
                    public void handle( FrekiCommand.SparseNode node ) throws IOException
                    {
                        Record record = node.after;
                        byte sizeExp = record.sizeExp();
                        SimpleStore store = mainStores.mainStore( sizeExp );
                        try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
                        {
                            store.write( cursor, record, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
                        }
                    }

                    @Override
                    public void handle( FrekiCommand.BigPropertyValue value ) throws IOException
                    {
                        try ( PageCursor cursor = bigValueStore.openWriteCursor( PageCursorTracer.NULL ) )
                        {
                            bigValueStore.write( cursor, ByteBuffer.wrap( value.bytes ), value.pointer );
                        }
                    }
                } );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }
}
