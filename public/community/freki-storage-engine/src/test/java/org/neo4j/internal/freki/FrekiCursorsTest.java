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

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.BufferOverflowException;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

@ExtendWith( {RandomExtension.class, EphemeralFileSystemExtension.class} )
abstract class FrekiCursorsTest
{
    @Inject
    RandomRule random;

    InMemoryTestStore store = new InMemoryTestStore( 0 );
    InMemoryTestStore largeStore = new InMemoryTestStore( 2 );
    MainStores stores = new MainStores( new SimpleStore[]{store, null, largeStore}, new InMemoryBigValueTestStore(), null );

    static long[] toLongArray( int[] labelIds )
    {
        return IntStream.of( labelIds ).mapToLong( v -> v ).toArray();
    }

    Node node()
    {
        return node( random.nextLong( 0, 0xFFFFFFFFFFL ) );
    }

    Node node( long id )
    {
        return new Node( id );
    }

    class Node
    {
        private final Record record;
        private final MutableNodeRecordData data;
        private final Consumer<StorageCommand> bigValueApplier = InMemoryBigValueTestStore.applyToStoreImmediately( stores.bigPropertyValueStore );

        Node( long id )
        {
            this( 0, id );
        }

        Node( int sizeExp, long id )
        {
            this.record = new Record( sizeExp, id );
            data = new MutableNodeRecordData( id );
            record.setFlag( FLAG_IN_USE, true );
        }

        long id()
        {
            return record.id;
        }

        Record store()
        {
            try ( PageCursor cursor = store.openWriteCursor() )
            {
                try
                {
                    data.serialize( record.dataForWriting(), stores.bigPropertyValueStore, bigValueApplier );
                }
                catch ( BufferOverflowException e )
                {
                    InMemoryTestStore largeStore = (InMemoryTestStore) stores.nextLargerMainStore( record.sizeExp() );
                    MutableNodeRecordData largeData = new MutableNodeRecordData( largeStore.nextId( PageCursorTracer.NULL ) );
                    data.movePropertiesAndRelationshipsTo( largeData );
                    try ( PageCursor largeCursor = largeStore.openWriteCursor() )
                    {
                        Record largeRecord = largeStore.newRecord( largeData.id );
                        largeRecord.setFlag( FLAG_IN_USE, true );
                        largeData.serialize( largeRecord.dataForWriting(), stores.bigPropertyValueStore, bigValueApplier );
                        largeStore.write( largeCursor, largeRecord, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
                    }

                    data.setForwardPointer( forwardPointer( largeStore.recordSizeExponential(), false, largeData.id ) );
                    data.serialize( record.dataForWriting(), stores.bigPropertyValueStore, bigValueApplier );
                }
                store.write( cursor, record, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
            }
            return record;
        }

        FrekiNodeCursor storeAndPlaceNodeCursorAt()
        {
            Record record = store();
            FrekiNodeCursor nodeCursor = new FrekiNodeCursor( stores, PageCursorTracer.NULL );
            nodeCursor.single( record.id );
            assertTrue( nodeCursor.next() );
            return nodeCursor;
        }

        Node inUse( boolean inUse )
        {
            record.setFlag( FLAG_IN_USE, inUse );
            return this;
        }

        Node labels( int... labelIds )
        {
            data.labels.addAll( labelIds );
            return this;
        }

        Node property( int propertyKeyId, Value value )
        {
            data.setNodeProperty( propertyKeyId, value );
            return this;
        }

        Node properties( IntObjectMap<Value> properties )
        {
            properties.forEachKeyValue( data::setNodeProperty );
            return this;
        }

        Node relationship( int type, Node otherNode )
        {
            return relationship( type, otherNode, IntObjectMaps.immutable.empty() );
        }

        Node relationship( int type, Node otherNode, IntObjectMap<Value> properties )
        {
            MutableNodeRecordData.Relationship relationship = data.createRelationship( null, otherNode.record.id, type );
            properties.forEachKeyValue( relationship::addProperty );
            if ( record.id != otherNode.record.id )
            {
                relationship = otherNode.data.createRelationship( relationship, record.id, type );
                properties.forEachKeyValue( relationship::addProperty );
            }
            return this;
        }
    }

    enum EntityAndPropertyConnector
    {
        DIRECT
                {
                    @Override
                    void connect( StorageEntityCursor entityCursor, StoragePropertyCursor propertyCursor )
                    {
                        entityCursor.properties( propertyCursor );
                    }
                },
        REFERENCE
                {
                    @Override
                    void connect( StorageEntityCursor entityCursor, StoragePropertyCursor propertyCursor )
                    {
                        if ( entityCursor instanceof StorageNodeCursor )
                        {
                            propertyCursor.initNodeProperties( entityCursor.propertiesReference() );
                        }
                        else
                        {
                            propertyCursor.initRelationshipProperties( entityCursor.propertiesReference() );
                        }
                    }
                },
        DIRECT_REFERENCE
                {
                    @Override
                    void connect( StorageEntityCursor entityCursor, StoragePropertyCursor propertyCursor )
                    {
                        if ( entityCursor instanceof StorageNodeCursor )
                        {
                            propertyCursor.initNodeProperties( (StorageNodeCursor) entityCursor );
                        }
                        else
                        {
                            propertyCursor.initRelationshipProperties( (StorageRelationshipCursor) entityCursor );
                        }
                    }
                };

        abstract void connect( StorageEntityCursor entityCursor, StoragePropertyCursor propertyCursor );
    }

    enum AllRelationshipsConnector
    {
        DIRECT_REFERENCE
                {
                    @Override
                    void connect( StorageNodeCursor nodeCursor, StorageRelationshipTraversalCursor relationshipCursor, RelationshipSelection selection )
                    {
                        relationshipCursor.init( nodeCursor, selection );
                    }
                },
        REFERENCE
                {
                    @Override
                    void connect( StorageNodeCursor nodeCursor, StorageRelationshipTraversalCursor relationshipCursor, RelationshipSelection selection )
                    {
                        relationshipCursor.init( nodeCursor.entityReference(), nodeCursor.relationshipsReference(), selection );
                    }
                };

        abstract void connect( StorageNodeCursor nodeCursor, StorageRelationshipTraversalCursor relationshipCursor, RelationshipSelection selection );
    }
}
