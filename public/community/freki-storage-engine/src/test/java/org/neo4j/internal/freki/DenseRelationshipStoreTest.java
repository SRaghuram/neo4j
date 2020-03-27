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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
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
                asSet( new PropertyKeyValue( 159, doubleValue( 10.45D ) ) ) ) );

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
        Rel out = new Rel( 100_000, nodeId, 123, 200_000, RelationshipDirection.OUTGOING, asSet( property ) );
        Rel in = new Rel( 100_001, nodeId, 123, 200_001, RelationshipDirection.INCOMING, asSet( property ) );
        try ( DenseRelationshipStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                updater.createRelationship( i, nodeId, i % 3, i, i % 2 == 0, IntObjectMaps.immutable.empty(), u -> u.after );
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
                updater.createRelationship( r, outgoing ? nodeId : otherNodeId, (r % 10) * random.nextInt( 1, 100 ), outgoing ? otherNodeId : nodeId, outgoing,
                        IntObjectMaps.immutable.empty(), v -> v.after );
            }
        }

        // when
        List<Rel> relationships = new ArrayList<>();
        for ( DenseRelationshipStore.RelationshipData relationship : loop( store.getRelationships( nodeId, ANY_RELATIONSHIP_TYPE, BOTH, NULL ) ) )
        {
            relationships.add( new Rel( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                    relationship.direction(), asSet( relationship.properties() ) ) );
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
        try ( PrefetchingResourceIterator<DenseRelationshipStore.RelationshipData> emptyRelationships = store.getRelationships( nodeId, ANY_RELATIONSHIP_TYPE,
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
                Set<StorageProperty> properties = new HashSet<>();
                int numProperties = random.nextInt( 3 );
                for ( int p = 0; p < numProperties; p++ )
                {
                    properties.add( new PropertyKeyValue( p, random.nextValue() ) );
                }
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

    private void assertRelationships( long sourceNodeId, int type, Direction direction, Set<Rel> expectedRelationships )
    {
        Set<Rel> readRelationships = new HashSet<>();
        try ( ResourceIterator<DenseRelationshipStore.RelationshipData> relationships = store.getRelationships( sourceNodeId, type, direction, NULL ) )
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

    private Set<StorageProperty> readProperties( DenseRelationshipStore.RelationshipData relationship )
    {
        Set<StorageProperty> readProperties = new HashSet<>();
        if ( relationship.hasProperties() )
        {
            Iterator<StorageProperty> props = relationship.properties();
            while ( props.hasNext() )
            {
                StorageProperty property = props.next();
                readProperties.add( new PropertyKeyValue( property.propertyKeyId(), property.value() ) );
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
        updater.createRelationship( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                relationship.direction() == RelationshipDirection.OUTGOING, serialize( relationship.properties ), u -> u.after );
    }

    private IntObjectMap<PropertyUpdate> serialize( Set<StorageProperty> properties )
    {
        MutableIntObjectMap<PropertyUpdate> serialized = IntObjectMaps.mutable.empty();
        properties.forEach( property -> serialized.put( property.propertyKeyId(), add( property.propertyKeyId(), serialize( property.value() ) ) ) );
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

    private static class Rel implements DenseRelationshipStore.RelationshipData
    {
        private final long internalId;
        private final long originNodeId;
        private final int type;
        private final long neighbourNodeId;
        private final RelationshipDirection direction;
        private final Set<StorageProperty> properties;

        Rel( long internalId, long originNodeId, int type, long neighbourNodeId, RelationshipDirection direction, Set<StorageProperty> properties )
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
            return properties.iterator();
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
    }
}
