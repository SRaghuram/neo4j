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

import org.apache.commons.lang3.mutable.MutableObject;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;

import org.neo4j.internal.freki.GraphUpdates.NodeUpdates;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.freki.InMemoryBigValueTestStore.applyToStoreImmediately;
import static org.neo4j.internal.freki.MutableNodeRecordData.externalRelationshipId;

@ExtendWith( {RandomExtension.class, EphemeralFileSystemExtension.class} )
abstract class FrekiCursorsTest
{
    @Inject
    RandomRule random;

    InMemoryTestStore store = new InMemoryTestStore( 0 );
    InMemoryTestStore largeStore = new InMemoryTestStore( 3 );
    MainStores stores = new MainStores( new SimpleStore[]{store, null, null, largeStore}, new InMemoryBigValueTestStore(), null );
    SingleThreadedCursorAccessPatternTracer accessPatternTracer = new SingleThreadedCursorAccessPatternTracer();
    FrekiCursorFactory cursorFactory = new FrekiCursorFactory( stores, accessPatternTracer );

    static long[] toLongArray( int[] labelIds )
    {
        return IntStream.of( labelIds ).mapToLong( v -> v ).toArray();
    }

    Node node()
    {
        return new Node( store.nextId( PageCursorTracer.NULL ) );
    }

    class Node
    {
        private final NodeUpdates updates;
        private long nextInternalId = 1;

        Node( long id )
        {
            updates = new NodeUpdates( id, stores, applyToStoreImmediately( stores.bigPropertyValueStore ), PageCursorTracer.NULL );
        }

        long id()
        {
            return updates.nodeId();
        }

        Record store()
        {
            try
            {
                MutableObject<Record> x1 = new MutableObject<>();
                updates.serialize(
                        ByteBuffer.wrap( new byte[stores.mainStore.recordDataSize()] ),
                        ByteBuffer.wrap( new byte[stores.largestMainStore().recordDataSize()] ), command ->
                        {
                            FrekiCommand.SparseNode sparseNode = (FrekiCommand.SparseNode) command;
                            Record record = sparseNode.after;
                            SimpleStore store = stores.mainStore( record.sizeExp() );
                            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
                            {
                                store.write( cursor, record, IdUpdateListener.IGNORE, PageCursorTracer.NULL );
                            }
                            catch ( IOException e )
                            {
                                throw new UncheckedIOException( e );
                            }
                            if ( record.sizeExp() == 0 )
                            {
                                x1.setValue( record );
                            }
                        } );
                return x1.getValue();
            }
            catch ( ConstraintViolationTransactionFailureException e )
            {
                throw new RuntimeException( e );
            }
        }

        FrekiNodeCursor storeAndPlaceNodeCursorAt()
        {
            Record record = store();
            FrekiNodeCursor nodeCursor = cursorFactory.allocateNodeCursor( PageCursorTracer.NULL );
            nodeCursor.single( record.id );
            assertTrue( nodeCursor.next() );
            return nodeCursor;
        }

        Node delete()
        {
            updates.delete();
            return this;
        }

        Node labels( int... labelIds )
        {
            MutableLongSet addedLabels = LongSets.mutable.empty();
            IntStream.of( labelIds ).forEach( addedLabels::add );
            updates.updateLabels( addedLabels, LongSets.immutable.empty() );
            return this;
        }

        Node property( int propertyKeyId, Value value )
        {
            updates.updateNodeProperties( singleton( new PropertyKeyValue( propertyKeyId, value ) ), emptyList(), IntSets.immutable.empty() );
            return this;
        }

        Node properties( IntObjectMap<Value> properties )
        {
            updates.updateNodeProperties( convertPropertiesMap( properties ), emptyList(), IntSets.immutable.empty() );
            return this;
        }

        Node relationship( int type, Node otherNode )
        {
            return relationship( type, otherNode, IntObjectMaps.immutable.empty() );
        }

        Node relationship( int type, Node otherNode, IntObjectMap<Value> properties )
        {
            createRelationship( type, otherNode, properties );
            return this;
        }

        long relationshipAndReturnItsId( int type, Node otherNode )
        {
            return relationshipAndReturnItsId( type, otherNode, IntObjectMaps.immutable.empty() );
        }

        long relationshipAndReturnItsId( int type, Node otherNode, IntObjectMap<Value> properties )
        {
            return createRelationship( type, otherNode, properties );
        }

        private long createRelationship( int type, Node otherNode, IntObjectMap<Value> properties )
        {
            long internalId = nextInternalId++;
            Collection<StorageProperty> addedProperties = convertPropertiesMap( properties );
            updates.createRelationship( internalId, otherNode.id(), type, true, addedProperties );
            if ( id() != otherNode.id() )
            {
                otherNode.updates.createRelationship( internalId, id(), type, false, addedProperties );
            }
            return externalRelationshipId( id(), internalId );
        }

        private Collection<StorageProperty> convertPropertiesMap( IntObjectMap<Value> properties )
        {
            Collection<StorageProperty> addedProperties = new ArrayList<>();
            properties.forEachKeyValue( ( key, value ) -> addedProperties.add( new PropertyKeyValue( key, value ) ) );
            return addedProperties;
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
