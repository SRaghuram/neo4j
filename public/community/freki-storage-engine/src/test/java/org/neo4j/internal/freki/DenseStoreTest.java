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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

@PageCacheExtension
class DenseStoreTest
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    private DenseStore store;

    @BeforeEach
    void start()
    {
        store = new DenseStore( pageCache, directory.file( "store" ), immediate(), false, PageCacheTracer.NULL, null );
    }

    @AfterEach
    void stop() throws IOException
    {
        store.close();
    }

    @Test
    void shouldSetAndGetProperties() throws IOException
    {
        // given
        long nodeId = 10;
        Set<StorageProperty> expectedProperties = asSet( new PropertyKeyValue( 4, intValue( 20 ) ), new PropertyKeyValue( 56, stringValue( "abc" ) ) );

        // when
        setProperties( nodeId, expectedProperties );

        // when
        assertProperties( nodeId, expectedProperties );
    }

    @Test
    void shouldChangeProperties() throws IOException
    {
        // given
        long nodeId = 10;
        setProperties( nodeId, asSet( new PropertyKeyValue( 56, stringValue( "abcdefg" ) ) ) );

        // when
        Set<StorageProperty> changedProperty = asSet( new PropertyKeyValue( 56, intValue( 123 ) ) );
        setProperties( nodeId, changedProperty );

        // then
        assertProperties( nodeId, changedProperty );
    }

    @Test
    void shouldRemoveProperties() throws IOException
    {
        // given
        long nodeId = 10;
        Set<StorageProperty> expectedProperties = asSet( new PropertyKeyValue( 4, intValue( 20 ) ), new PropertyKeyValue( 56, stringValue( "abc" ) ) );
        setProperties( nodeId, expectedProperties );

        // when
        removeProperties( nodeId, 56 );
        expectedProperties.remove( new PropertyKeyValue( 56, stringValue( "abc" ) ) );

        // then
        assertProperties( nodeId, expectedProperties );
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
        try ( DenseStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int i = 0; i < 10_000; i++ )
            {
                updater.createRelationship( i, nodeId, i % 3, i, i % 2 == 0, emptyList() );
            }
            createRelationship( updater, out );
            createRelationship( updater, in );
        }

        // then
        assertRelationships( nodeId, type, BOTH, asSet( out, in ) );
        assertRelationships( nodeId, type, OUTGOING, asSet( out ) );
        assertRelationships( nodeId, type, INCOMING, asSet( in ) );
    }

    private void assertRelationships( long sourceNodeId, int type, Direction direction, Set<Rel> expectedRelationships )
    {
        Set<Rel> readRelationships = new HashSet<>();
        try ( ResourceIterator<DenseStore.RelationshipData> relationships = store.getRelationships( sourceNodeId, type, direction, NULL ) )
        {
            while ( relationships.hasNext() )
            {
                DenseStore.RelationshipData relationship = relationships.next();
                readRelationships.add( new Rel( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                        relationship.direction(), readProperties( relationship ) ) );
            }
            assertThat( readRelationships ).isEqualTo( expectedRelationships );
        }
    }

    private Set<StorageProperty> readProperties( DenseStore.RelationshipData relationship )
    {
        Set<StorageProperty> readProperties = new HashSet<>();
        Iterator<StorageProperty> props = relationship.properties();
        while ( props.hasNext() )
        {
            StorageProperty property = props.next();
            readProperties.add( new PropertyKeyValue( property.propertyKeyId(), property.value() ) );
        }
        return readProperties;
    }

    private void createRelationships( Set<Rel> expectedRelationships ) throws IOException
    {
        try ( DenseStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( Rel relationship : expectedRelationships )
            {
                createRelationship( updater, relationship );
            }
        }
    }

    private void createRelationship( DenseStore.Updater updater, Rel relationship )
    {
        updater.createRelationship( relationship.internalId(), relationship.originNodeId(), relationship.type(), relationship.neighbourNodeId(),
                relationship.direction() == RelationshipDirection.OUTGOING, relationship.properties );
    }

    private void removeProperties( long nodeId, int... propertyKeys ) throws IOException
    {
        try ( DenseStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( int propertyKey : propertyKeys )
            {
                updater.removeProperty( nodeId, propertyKey );
            }
        }
    }

    private void assertProperties( long nodeId, Set<StorageProperty> expectedProperties )
    {
        Set<StorageProperty> readProperties = new HashSet<>();
        try ( ResourceIterator<StorageProperty> properties = store.getProperties( nodeId, NULL ) )
        {
            while ( properties.hasNext() )
            {
                StorageProperty property = properties.next();
                readProperties.add( new PropertyKeyValue( property.propertyKeyId(), property.value() ) );
            }
        }

        // then
        assertThat( readProperties ).isEqualTo( expectedProperties );
    }

    private void setProperties( long nodeId, Set<StorageProperty> expectedProperties ) throws IOException
    {
        try ( DenseStore.Updater updater = store.newUpdater( NULL ) )
        {
            for ( StorageProperty property : expectedProperties )
            {
                updater.setProperty( nodeId, property );
            }
        }
    }

    private static class Rel implements DenseStore.RelationshipData
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

        @Override
        public Iterator<StorageProperty> properties()
        {
            return properties.iterator();
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
