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

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.IntStream;

import org.neo4j.internal.freki.GraphUpdates.NodeUpdates;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageEntityCursor;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.freki.MinimalTestFrekiTransactionApplier.NO_MONITOR;
import static org.neo4j.internal.freki.MutableNodeData.externalRelationshipId;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@ExtendWith( {RandomExtension.class, EphemeralFileSystemExtension.class} )
abstract class FrekiCursorsTest
{
    @Inject
    RandomRule random;

    InMemoryTestStore[] mainStores = new InMemoryTestStore[]{new InMemoryTestStore( 0 ), new InMemoryTestStore( 1 ), null, new InMemoryTestStore( 3 )};
    InMemoryTestStore store = mainStores[0];
    InMemoryTestStore largeStore = mainStores[3];
    InMemoryBigValueTestStore bigPropertyValueStore = new InMemoryBigValueTestStore();
    InMemoryDenseRelationshipTestStore denseStore = new InMemoryDenseRelationshipTestStore( bigPropertyValueStore );
    MainStores stores = new MainStores( mainStores, bigPropertyValueStore, denseStore );
    SingleThreadedCursorAccessPatternTracer accessPatternTracer = new SingleThreadedCursorAccessPatternTracer();
    FrekiCursorFactory cursorFactory = new FrekiCursorFactory( stores, accessPatternTracer );
    FrekiAnalysis analysis = new FrekiAnalysis( stores );

    static long[] toLongArray( int[] labelIds )
    {
        return IntStream.of( labelIds ).mapToLong( v -> v ).toArray();
    }

    Node node()
    {
        return new Node( store.nextId( NULL ) );
    }

    Node existingNode( long id )
    {
        return new Node( id );
    }

    class Node
    {
        private final NodeUpdates updates;
        private final GraphUpdates graphUpdates;
        private final FrekiRelationshipIdGenerator relationshipIdGenerator;

        Node( long id )
        {
            graphUpdates = new GraphUpdates( stores, NULL, EmptyMemoryTracker.INSTANCE );
            if ( !store.exists( null, id ) )
            {
                graphUpdates.create( id );
            }
            updates = graphUpdates.getOrLoad( id );
            relationshipIdGenerator = new FrekiRelationshipIdGenerator( stores, NULL, EmptyMemoryTracker.INSTANCE );
        }

        long id()
        {
            return updates.nodeId();
        }

        long store()
        {
            return store( NO_MONITOR );
        }

        long store( FrekiCommand.Dispatcher monitor )
        {
            try
            {

                graphUpdates.extractUpdates( new MinimalTestFrekiTransactionApplier( stores, monitor ) );
                return updates.nodeId();
            }
            catch ( ConstraintViolationTransactionFailureException e )
            {
                throw new RuntimeException( e );
            }
        }

        FrekiNodeCursor storeAndPlaceNodeCursorAt()
        {
            long id = store();
            FrekiNodeCursor nodeCursor = cursorFactory.allocateNodeCursor( NULL );
            nodeCursor.single( id );
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
            updates.updateLabels( intsAsLongSet( labelIds ), LongSets.immutable.empty() );
            return this;
        }

        Node removeLabels( int... labelIds )
        {
            updates.updateLabels( LongSets.immutable.empty(), intsAsLongSet( labelIds ) );
            return this;
        }

        private MutableLongSet intsAsLongSet( int[] labelIds )
        {
            MutableLongSet addedLabels = LongSets.mutable.empty();
            IntStream.of( labelIds ).forEach( addedLabels::add );
            return addedLabels;
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

        Node removeProperty( int key )
        {
            updates.updateNodeProperties( emptyList(), emptyList(), IntSets.immutable.of( key ) );
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
            long internalId = relationshipIdGenerator.reserveInternalRelationshipId( id() );
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

    static PhysicalLayout layout()
    {
        return new PhysicalLayout();
    }

    void assertPhysicalLayout( long nodeId, PhysicalLayout expectedLayout )
    {
        PhysicalLayout actualLayout = capturePhysicalLayout( nodeId );
        for ( int slot = 0; slot < expectedLayout.lists.length; slot++ )
        {
            if ( expectedLayout.lists[slot] != null )
            {
                assertThat( actualLayout.lists[slot] ).isEqualTo( expectedLayout.lists[slot] );
            }
        }
        if ( expectedLayout.isDense != null )
        {
            assertThat( actualLayout.isDense ).isEqualTo( expectedLayout.isDense );
        }
    }

    boolean matchesPhysicalLayout( long nodeId, PhysicalLayout expectedLayout )
    {
        PhysicalLayout actualLayout = capturePhysicalLayout( nodeId );
        for ( int slot = 0; slot < expectedLayout.lists.length; slot++ )
        {
            if ( expectedLayout.lists[slot] != null && !expectedLayout.lists[slot].equals( actualLayout.lists[slot] ) )
            {
                return false;
            }
        }
        return expectedLayout.isDense == null || expectedLayout.isDense.equals( actualLayout.isDense );
    }

    PhysicalLayout capturePhysicalLayout( long nodeId )
    {
        PhysicalLayout actualLayout = new PhysicalLayout();
        analysis.visitPhysicalPartsLayout( nodeId, new FrekiAnalysis.PhysicalPartsLayoutVisitor()
        {
            @Override
            public void xRecord( int sizeExp, long id, Header header, boolean first, boolean last )
            {
                note( PhysicalLayout.LABELS, header.hasMark( Header.FLAG_LABELS ), sizeExp );
                note( PhysicalLayout.PROPERTIES, header.hasMark( Header.OFFSET_PROPERTIES ), sizeExp );
                note( PhysicalLayout.DEGREES, header.hasMark( Header.OFFSET_DEGREES ), sizeExp );
                note( PhysicalLayout.RELATIONSHIPS, header.hasMark( Header.OFFSET_RELATIONSHIPS ), sizeExp );
            }

            private void note( int slot, boolean hasMark, int sizeExp )
            {
                if ( hasMark )
                {
                    actualLayout.add( slot, sizeExp );
                }
            }
        } );
        return actualLayout;
    }

    static class PhysicalLayout
    {
        private static final int LABELS = 0;
        private static final int PROPERTIES = 1;
        private static final int DEGREES = 2;
        private static final int RELATIONSHIPS = 3;

        private MutableIntList[] lists = new MutableIntList[4];
        private Boolean isDense;

        private MutableIntList list( int index )
        {
            if ( lists[index] == null )
            {
                lists[index] = IntLists.mutable.empty();
            }
            return lists[index];
        }

        PhysicalLayout labels( int... sizeExp )
        {
            return add( LABELS, sizeExp );
        }

        PhysicalLayout properties( int... sizeExp )
        {
            return add( PROPERTIES, sizeExp );
        }

        PhysicalLayout degrees( int... sizeExp )
        {
            return add( DEGREES, sizeExp );
        }

        PhysicalLayout relationships( int... sizeExp )
        {
            return add( RELATIONSHIPS, sizeExp );
        }

        private PhysicalLayout add( int slot, int... sizeExp )
        {
            list( slot ).addAll( sizeExp );
            return this;
        }

        PhysicalLayout isDense( boolean shouldBeDense )
        {
            return this;
        }
    }

    MutableIntObjectMap<Value> readProperties( FrekiPropertyCursor propertyCursor )
    {
        MutableIntObjectMap<Value> readProperties = IntObjectMaps.mutable.empty();
        while ( propertyCursor.next() )
        {
            readProperties.put( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }
        return readProperties;
    }
}
