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
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.InMemoryBigValueTestStore.applyToStoreImmediately;
import static org.neo4j.internal.freki.PropertyUpdate.add;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.loop;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class DenseRelationshipStoreTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    private InMemoryBigValueTestStore bigPropertyValueStore = new InMemoryBigValueTestStore();
    private DenseRelationshipStore store;

    @BeforeEach
    void start()
    {
        store = new DenseRelationshipStore( pageCache, directory.file( "store" ), immediate(), false, PageCacheTracer.NULL, bigPropertyValueStore );
    }

    @AfterEach
    void stop() throws IOException
    {
        store.close();
    }

    @Test
    void shouldCreateOutgoingRelationship() throws IOException
    {
        // given
        long sourceNodeId = 123;
        long targetNodeId = 456;
        long internalId = 1;
        int type = 89;
        Set<Rel> expectedRelationships = asSet( new Rel( internalId, sourceNodeId, type, targetNodeId, RelationshipDirection.OUTGOING,
                properties( new PropertyKeyValue( 159, doubleValue( 10.45D ) ) ) ) );

        // when
        createRelationships( expectedRelationships );

        // then
        assertRelationships( sourceNodeId, type, BOTH, expectedRelationships );
    }

    @Test
    void shouldCreateLotsOfRelationshipsAndGetBackTheFewOnesOfACertainTypeAndDirection() throws IOException
    {
        // given
        long nodeId = 1010;
        int type = 123;
        StorageProperty property = new PropertyKeyValue( 1928, intValue( 10 ) );
        Rel out = new Rel( 100_000, nodeId, 123, 200_000, RelationshipDirection.OUTGOING, properties( property ) );
        Rel in = new Rel( 100_001, nodeId, 123, 200_001, RelationshipDirection.INCOMING, properties( property ) );
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                updater.insertRelationship( i, nodeId, i % 3, i, i % 2 == 0, IntObjectMaps.immutable.empty(), u -> u.after );
            }
            createRelationship( updater, out );
            createRelationship( updater, in );
        }

        // then
        assertRelationships( nodeId, type, BOTH, asSet( out, in ) );
        assertRelationships( nodeId, type, OUTGOING, asSet( out ) );
        assertRelationships( nodeId, type, INCOMING, asSet( in ) );
    }

    @Test
    void shouldDeleteNodeWithLotsOfRelationships() throws IOException
    {
        // given
        long nodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int r = 0; r < 1_000; r++ )
            {
                boolean outgoing = random.nextBoolean();
                long otherNodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
                updater.insertRelationship( r, outgoing ? nodeId : otherNodeId, (r % 10) * random.nextInt( 1, 100 ), outgoing ? otherNodeId : nodeId, outgoing,
                        IntObjectMaps.immutable.empty(), v -> v.after );
            }
        }

        // when
        List<Rel> relationships = new ArrayList<>();
        for ( DenseRelationshipStore.RelationshipData relationship : loop( store.getRelationships( nodeId, ANY_RELATIONSHIP_TYPE, BOTH, NULL ) ) )
        {
            relationships.add( new Rel( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                    relationship.direction(), properties( relationship.properties() ) ) );
        }
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( Rel relationship : relationships )
            {
                updater.deleteRelationship( relationship.internalId, relationship.originNodeId, relationship.type, relationship.neighbourNodeId,
                        relationship.direction != RelationshipDirection.INCOMING );
            }
        }

        // then
        try ( ResourceIterator<DenseRelationshipStore.RelationshipData> emptyRelationships = store.getRelationships( nodeId, ANY_RELATIONSHIP_TYPE,
                BOTH, NULL ) )
        {
            assertThat( emptyRelationships.hasNext() ).isFalse();
        }
    }

    @Test
    void shouldLoadSerializedPropertiesWhenDeleteRelationships() throws IOException
    {
        // given
        long nodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
        Set<Rel> relationships = new HashSet<>();
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                boolean outgoing = random.nextBoolean();
                long otherNodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
                Map<Integer,Value> properties = randomProperties( random.nextInt( 3 ) );
                Rel relationship = new Rel( i, nodeId, random.nextInt( 4 ), otherNodeId,
                        outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING, properties );
                createRelationship( updater, relationship );
                relationships.add( relationship );
            }
        }
        assertRelationships( nodeId, ANY_RELATIONSHIP_TYPE, BOTH, relationships );

        while ( !relationships.isEmpty() )
        {
            // when
            Rel relationship = random.among( new ArrayList<>( relationships ) );
            try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
            {
                updater.deleteRelationship( relationship.internalId, relationship.originNodeId, relationship.type, relationship.neighbourNodeId,
                        relationship.isOutgoing() );
            }
            relationships.remove( relationship );

            // then
            assertRelationships( nodeId, ANY_RELATIONSHIP_TYPE, BOTH, relationships );
        }
    }

    @Test
    void shouldGetRelationshipsToSpecificNeighbourNode() throws IOException
    {
        // given
        long nodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
        List<Rel> relationships = new ArrayList<>();
        int numOtherNodes = 100;
        int numTypes = 4;
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < numOtherNodes * 10; i++ )
            {
                long otherNodeId = i % numOtherNodes;
                boolean outgoing = otherNodeId == nodeId || random.nextBoolean();
                int type = random.nextInt( numTypes );
                long originNodeId = outgoing ? nodeId : otherNodeId;
                long neighbourNodeId = outgoing ? otherNodeId : nodeId;
                updater.insertRelationship( i, originNodeId, type, neighbourNodeId, outgoing, IntObjectMaps.immutable.empty(), p -> p.after );
                relationships.add( new Rel( i, originNodeId, type, neighbourNodeId, outgoing ? RelationshipDirection.OUTGOING : RelationshipDirection.INCOMING,
                        emptyMap() ) );
            }
        }

        // when/then
        for ( Direction direction : Direction.values() )
        {
            for ( int type = 0; type < numTypes; type++ )
            {
                for ( long otherNodeId = 0; otherNodeId < numOtherNodes; otherNodeId++ )
                {
                    assertRelationships( store.getRelationships( nodeId, type, direction, otherNodeId, NULL ),
                            subset( relationships, type, direction, otherNodeId ) );
                }
            }
        }
    }

    @Test
    void shouldUpdateRelationshipProperties() throws IOException
    {
        // given
        long nodeId = random.nextLong( 0xFFFFFF_FFFFFFFFL );
        List<Rel> relationships = new ArrayList<>();
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < 1; i++ )
            {
                Rel relationship = new Rel( i, nodeId, 0, 100 + i, RelationshipDirection.OUTGOING, randomProperties( 5 ) );
                relationships.add( relationship );
                relationship.insert( updater );
            }
        }

        // when
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            Rel relationship = random.among( relationships );
            int numChanged = random.nextInt( 1, 3 );
            for ( int i = 0; i < numChanged; i++ )
            {
                int key = random.among( relationship.properties.keySet().toArray( new Integer[0] ) );
                Value existingValue = relationship.properties.get( key );
                Value updatedValue;
                do
                {
                    updatedValue = random.nextValue();
                }
                while ( updatedValue.equals( existingValue ) );
                relationship.properties.put( key, updatedValue );
            }
            int numRemoved = random.nextInt( 1, 3 );
            for ( int i = 0; i < numRemoved; i++ )
            {
                int key = random.among( relationship.properties.keySet().toArray( new Integer[0] ) );
                relationship.properties.remove( key );
            }
            int numAdded = random.nextInt( 1, 3 );
            for ( int i = 0; i < numAdded; i++ )
            {
                relationship.properties.put( 20 + i, random.nextValue() );
            }
            relationship.insert( updater );
        }

        // then
        assertRelationships( nodeId, -1, BOTH, asSet( relationships.iterator() ) );
    }

    private Map<Integer,Value> properties( StorageProperty... properties )
    {
        Map<Integer,Value> map = new HashMap<>();
        for ( StorageProperty property : properties )
        {
            map.put( property.propertyKeyId(), property.value() );
        }
        return map;
    }

    private Map<Integer,Value> properties( Iterator<StorageProperty> properties )
    {
        Map<Integer,Value> map = new HashMap<>();
        while ( properties.hasNext() )
        {
            StorageProperty property = properties.next();
            map.put( property.propertyKeyId(), property.value() );
        }
        return map;
    }

    private Map<Integer,Value> randomProperties( int numProperties )
    {
        Map<Integer,Value> properties = new HashMap<>();
        for ( int p = 0; p < numProperties; p++ )
        {
            properties.put( p, random.nextValue() );
        }
        return properties;
    }

    private Set<Rel> subset( List<Rel> relationships, int type, Direction direction, long otherNodeId )
    {
        return relationships.stream()
                .filter( r -> r.type == type && (direction == BOTH || r.isOutgoing() == (direction == OUTGOING)) && r.neighbourNodeId == otherNodeId )
                .collect( Collectors.toSet() );
    }

    private void assertRelationships( long sourceNodeId, int type, Direction direction, Set<Rel> expectedRelationships )
    {
        assertRelationships( store.getRelationships( sourceNodeId, type, direction, NULL ), expectedRelationships );
    }

    private void assertRelationships( ResourceIterator<DenseRelationshipStore.RelationshipData> relationships, Set<Rel> expectedRelationships )
    {
        Set<Rel> readRelationships = new HashSet<>();
        try ( relationships )
        {
            while ( relationships.hasNext() )
            {
                DenseRelationshipStore.RelationshipData relationship = relationships.next();
                readRelationships.add( new Rel( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                        relationship.direction(), readProperties( relationship ) ) );
            }
            assertThat( readRelationships ).isEqualTo( expectedRelationships );
        }
    }

    private Map<Integer,Value> readProperties( DenseRelationshipStore.RelationshipData relationship )
    {
        Map<Integer,Value> readProperties = new HashMap<>();
        if ( relationship.hasProperties() )
        {
            Iterator<StorageProperty> props = relationship.properties();
            while ( props.hasNext() )
            {
                StorageProperty property = props.next();
                readProperties.put( property.propertyKeyId(), property.value() );
            }
        }
        return readProperties;
    }

    private void createRelationships( Set<Rel> expectedRelationships ) throws IOException
    {
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( Rel relationship : expectedRelationships )
            {
                createRelationship( updater, relationship );
            }
        }
    }

    private void createRelationship( DenseRelationshipStore.Updater updater, Rel relationship )
    {
        updater.insertRelationship( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                relationship.direction() == RelationshipDirection.OUTGOING, serialize( relationship.properties ), u -> u.after );
    }

    private IntObjectMap<PropertyUpdate> serialize( Map<Integer,Value> properties )
    {
        MutableIntObjectMap<PropertyUpdate> serialized = IntObjectMaps.mutable.empty();
        properties.forEach( ( key, value ) -> serialized.put( key, add( key, serialize( value ) ) ) );
        return serialized;
    }

    private ByteBuffer serialize( Value value )
    {
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        PropertyValueFormat format = new PropertyValueFormat( bigPropertyValueStore, applyToStoreImmediately( bigPropertyValueStore ), buffer );
        value.writeTo( format );
        buffer.flip();
        return buffer;
    }

    private class Rel implements DenseRelationshipStore.RelationshipData
    {
        private final long internalId;
        private final long originNodeId;
        private final int type;
        private final long neighbourNodeId;
        private final RelationshipDirection direction;
        private final Map<Integer,Value> properties;

        Rel( long internalId, long originNodeId, int type, long neighbourNodeId, RelationshipDirection direction, Map<Integer,Value> properties )
        {
            this.internalId = internalId;
            this.originNodeId = originNodeId;
            this.type = type;
            this.neighbourNodeId = neighbourNodeId;
            this.direction = direction;
            this.properties = properties;
        }

        @Override
        public long internalId()
        {
            return internalId;
        }

        @Override
        public long originNodeId()
        {
            return originNodeId;
        }

        @Override
        public long neighbourNodeId()
        {
            return neighbourNodeId;
        }

        @Override
        public int type()
        {
            return type;
        }

        @Override
        public RelationshipDirection direction()
        {
            return direction;
        }

        boolean isOutgoing()
        {
            return direction != RelationshipDirection.INCOMING;
        }

        @Override
        public Iterator<StorageProperty> properties()
        {
            return Iterators.map( entry -> new PropertyKeyValue( entry.getKey(), entry.getValue() ), properties.entrySet().iterator() );
        }

        @Override
        public boolean hasProperties()
        {
            return !properties.isEmpty();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Rel rel = (Rel) o;
            return internalId == rel.internalId && originNodeId == rel.originNodeId && type == rel.type && neighbourNodeId == rel.neighbourNodeId &&
                    direction == rel.direction && Objects.equals( properties, rel.properties );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( internalId, originNodeId, type, neighbourNodeId, direction, properties );
        }

        @Override
        public String toString()
        {
            return "Rel{" + originNodeId + (direction == RelationshipDirection.OUTGOING ? "-[" + type + "]>" : "<[" + type + "]-") + neighbourNodeId +
                    ", props=" + properties + "}";
        }

        void insert( DenseRelationshipStore.Updater updater )
        {
            updater.insertRelationship( internalId, originNodeId, type, neighbourNodeId, isOutgoing(), serialize( properties ), u -> u.after );
        }
    }
}
