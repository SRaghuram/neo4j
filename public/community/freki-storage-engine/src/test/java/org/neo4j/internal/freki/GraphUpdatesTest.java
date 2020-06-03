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

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.freki.MinimalTestFrekiTransactionApplier.NO_MONITOR;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

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
            GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
            updates.create( nodeId );
            extractAndApplyUpdates( updates, NO_MONITOR );
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
            GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
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
            extractAndApplyUpdates( updates, NO_MONITOR );
        }
    }

    @Test
    void shouldDeleteOverwrittenNodePropertyBigValueRecords() throws ConstraintViolationTransactionFailureException
    {
        // given
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        int key = 0;
        updates.create( nodeId );
        GraphUpdates.NodeUpdates node = updates.getOrLoad( nodeId );
        node.updateNodeProperties( singletonList( new PropertyKeyValue( key, random.nextAlphaNumericTextValue( 100, 100 ) ) ),
                emptyList(), IntSets.immutable.empty() );
        BigValueCounter monitor = new BigValueCounter();
        extractAndApplyUpdates( updates, monitor );
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( 0 );

        // when
        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        node = updates.getOrLoad( nodeId );
        node.updateNodeProperties( emptyList(), singletonList( new PropertyKeyValue( key, stringValue( "abc" ) ) ), IntSets.immutable.empty() );
        extractAndApplyUpdates( updates, monitor );

        // then
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( monitor.numCreated.intValue() );
    }

    @Test
    void shouldDeleteDeletedNodeBigValueRecords() throws ConstraintViolationTransactionFailureException
    {
        // given
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        int key = 0;
        updates.create( nodeId );
        GraphUpdates.NodeUpdates node = updates.getOrLoad( nodeId );
        node.updateNodeProperties( singletonList( new PropertyKeyValue( key, random.nextAlphaNumericTextValue( 100, 100 ) ) ),
                emptyList(), IntSets.immutable.empty() );
        BigValueCounter monitor = new BigValueCounter();
        extractAndApplyUpdates( updates, monitor );
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( 0 );

        // when
        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        node = updates.getOrLoad( nodeId );
        node.delete();
        extractAndApplyUpdates( updates, monitor );

        // then
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( monitor.numCreated.intValue() );
    }

    @Test
    void shouldDeleteOverwrittenRelationshipPropertyBigValueRecords() throws ConstraintViolationTransactionFailureException
    {
        // given
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        int key = 0;
        int type = 0;
        updates.create( nodeId );
        GraphUpdates.NodeUpdates node = updates.getOrLoad( nodeId );
        long internalId = 1;
        node.createRelationship( internalId, nodeId, type, true, singletonList( new PropertyKeyValue( key, random.nextAlphaNumericTextValue( 100, 100 ) ) ) );
        BigValueCounter monitor = new BigValueCounter();
        extractAndApplyUpdates( updates, monitor );
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( 0 );

        // when
        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        node = updates.getOrLoad( nodeId );
        node.updateRelationshipProperties( internalId, type, nodeId, true, emptyList(), singletonList( new PropertyKeyValue( key, intValue( 10 ) ) ),
                IntSets.immutable.empty() );
        extractAndApplyUpdates( updates, monitor );

        // then
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( monitor.numCreated.intValue() );
    }

    @Test
    void shouldDeleteDeletedRelationshipBigValueRecords() throws ConstraintViolationTransactionFailureException
    {
        // given
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        int key = 0;
        int type = 0;
        updates.create( nodeId );
        GraphUpdates.NodeUpdates node = updates.getOrLoad( nodeId );
        long internalId = 1;
        node.createRelationship( internalId, nodeId, type, true, singletonList( new PropertyKeyValue( key, random.nextAlphaNumericTextValue( 100, 100 ) ) ) );
        BigValueCounter monitor = new BigValueCounter();
        extractAndApplyUpdates( updates, monitor );
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( 0 );

        // when
        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        node = updates.getOrLoad( nodeId );
        node.deleteRelationship( internalId, type, nodeId, true );
        extractAndApplyUpdates( updates, monitor );

        // then
        assertThat( monitor.numCreated.intValue() ).isGreaterThan( 0 );
        assertThat( monitor.numDeleted.intValue() ).isEqualTo( monitor.numCreated.intValue() );
    }

    @Test
    void shouldPlaceDenseNodeCommandsFirst() throws ConstraintViolationTransactionFailureException
    {
        // given
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );

        // when
        for ( int i = 0; i < 5; i++ )
        {
            long nodeId = mainStores.mainStore.nextId( NULL );
            updates.create( nodeId );
            updates.getOrLoad( nodeId ).updateNodeProperties( singletonList( new PropertyKeyValue( 9, random.nextValue() ) ), emptyList(),
                    IntSets.immutable.empty() );
        }
        long denseNodeId = mainStores.mainStore.nextId( NULL );
        updates.create( denseNodeId );
        GraphUpdates.NodeUpdates denseNode = updates.getOrLoad( denseNodeId );
        for ( int i = 0; i < 200; i++ )
        {
            denseNode.createRelationship( i + 1, mainStores.mainStore.nextId( NULL ), 0, true, singletonList( new PropertyKeyValue( 0, intValue( 100 ) ) ) );
        }

        // then
        MutableBoolean hasSeenDense = new MutableBoolean();
        MutableBoolean hasSeenOther = new MutableBoolean();
        updates.extractUpdates( command ->
        {
            boolean isDenseCommand = command instanceof FrekiCommand.DenseNode;
            if ( isDenseCommand )
            {
                hasSeenDense.setTrue();
                assertThat( hasSeenOther.booleanValue() ).isFalse();
            }
            else
            {
                hasSeenOther.setTrue();
                assertThat( hasSeenDense.booleanValue() ).isTrue();
            }
        } );
        assertThat( hasSeenDense.booleanValue() ).isTrue();
        assertThat( hasSeenOther.booleanValue() ).isTrue();
    }

    @Test
    void shouldNotUpdateVersionOnChangesInsideX1Record() throws ConstraintViolationTransactionFailureException
    {
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        updates.create( nodeId );

        extractAndApplyUpdates( updates, new VersionUpdate( false ) );

        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        updates.getOrLoad( nodeId ).updateLabels( LongSets.immutable.of( 1,2,3 ), LongSets.immutable.empty() );
        updates.getOrLoad( nodeId )
                .updateNodeProperties( singletonList( new PropertyKeyValue( 9, random.nextValue() ) ), emptyList(), IntSets.immutable.empty() );

        extractAndApplyUpdates( updates, new VersionUpdate( false ) );
    }

    @Test
    void shouldNotUpdateVersionOnChangesInsideXLRecord() throws ConstraintViolationTransactionFailureException
    {
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        updates.create( nodeId );
        MutableLongSet manyLabels = LongSets.mutable.empty();
        for ( int i = 0; i < 200; i++ )
        {
            manyLabels.add( i + 4 );
        }
        updates.getOrLoad( nodeId ).updateLabels( manyLabels, LongSets.immutable.empty() );

        extractAndApplyUpdates( updates, layoutChange( false, new int[] {}, new int[] {1,4} ) );

        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        updates.getOrLoad( nodeId ).updateLabels( LongSets.immutable.of( 1,2,3 ), LongSets.immutable.empty() );

        extractAndApplyUpdates( updates, layoutChange( false, new int[] {4}, new int[] {4} ) ); // <-- No change means x1 is skipped!

    }

    @Test
    void shouldNotUpdateVersionOnPartRemoval() throws ConstraintViolationTransactionFailureException
    {
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        updates.create( nodeId );
        MutableLongSet manyLabels = LongSets.mutable.empty();
        for ( int i = 0; i < 200; i++ )
        {
            manyLabels.add( i + 4 );
        }
        updates.getOrLoad( nodeId ).updateLabels( manyLabels, LongSets.immutable.empty() );

        extractAndApplyUpdates( updates, layoutChange( false, new int[] {}, new int[] {1,4} ) );

        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        updates.getOrLoad( nodeId ).updateLabels( LongSets.immutable.empty(), manyLabels );

        extractAndApplyUpdates( updates, layoutChange( false, new int[] {1,4}, new int[] {1} ) ); // <-- No change means x1 is skipped!

    }

    @Test
    void shouldUpdateVersionOnChangesSwitchingRecord() throws ConstraintViolationTransactionFailureException
    {
        GraphUpdates updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        long nodeId = mainStores.mainStore.nextId( NULL );
        updates.create( nodeId );

        updates.getOrLoad( nodeId ).updateLabels( LongSets.immutable.of( 1,2,3 ), LongSets.immutable.empty() );
        extractAndApplyUpdates( updates, layoutChange( false, new int[] {}, new int[] {1} ) );
        updates = new GraphUpdates( mainStores, NULL, INSTANCE );

        MutableLongSet manyLabels = LongSets.mutable.empty();
        for ( int i = 0; i < 200; i++ )
        {
            manyLabels.add( i + 4 );
        }
        updates.getOrLoad( nodeId ).updateLabels( manyLabels, LongSets.immutable.of( 1,2,3 ) );
        extractAndApplyUpdates( updates, layoutChange( false, new int[] {1}, new int[] {1, 4} ) ); // this should not get a version change!

        updates = new GraphUpdates( mainStores, NULL, INSTANCE );
        MutableLongSet moreLabels = LongSets.mutable.empty();
        for ( int i = 0; i < 400; i++ )
        {
            moreLabels.add( i + 300 );
        }
        updates.getOrLoad( nodeId ).updateLabels( moreLabels, LongSets.immutable.empty() );
        extractAndApplyUpdates( updates, layoutChange( false, new int[] {1, 4}, new int[] {1, 8} ) );
    }

    private StorageProperty randomPropertyValue( int propertyKey )
    {
        return new PropertyKeyValue( propertyKey, random.nextValue() );
    }

    private void extractAndApplyUpdates( GraphUpdates updates, FrekiCommand.Dispatcher monitor ) throws ConstraintViolationTransactionFailureException
    {
        updates.extractUpdates( new MinimalTestFrekiTransactionApplier( mainStores, monitor ) );
    }

    static FrekiCommand.Dispatcher.Adapter layoutChange( boolean expectVersionChange, int[] beforeSizeX, int[] afterSizeX )
    {
        return new FrekiCommand.Dispatcher.Adapter()
        {
            private final Adapter[] adapters = new Adapter[] {new VersionUpdate( expectVersionChange ), new LayoutUpdate( beforeSizeX, afterSizeX )};
            @Override
            public void handle( FrekiCommand.SparseNode node ) throws IOException
            {
                for ( Adapter adapter : adapters )
                {
                    adapter.handle( node );
                }
            }
        };
    }

    private static class LayoutUpdate extends FrekiCommand.Dispatcher.Adapter
    {
        private final Integer[] beforeRecordsSizeX;
        private final Integer[] afterRecordsSizeX;

        LayoutUpdate( int[] beforeRecordsSizeX, int[] afterRecordsSizeX )
        {
            this.beforeRecordsSizeX = IntStream.of( beforeRecordsSizeX ).boxed().toArray( Integer[]::new );
            this.afterRecordsSizeX = IntStream.of( afterRecordsSizeX ).boxed().toArray( Integer[]::new );
        }

        @Override
        public void handle( FrekiCommand.SparseNode node )
        {
            List<Integer> actualBefore = new ArrayList<>();
            List<Integer> actualAfter = new ArrayList<>();
            for ( FrekiCommand.RecordChange recordChange : node )
            {
                if ( recordChange.before != null )
                {
                    actualBefore.add( Record.recordXFactor( recordChange.before.sizeExp() ) );
                }
                if ( recordChange.after != null )
                {
                    actualAfter.add( Record.recordXFactor( recordChange.after.sizeExp() ) );
                }
            }
            assertThat( actualBefore ).as( "Should have correct before layout" ).containsExactlyInAnyOrder( beforeRecordsSizeX );
            assertThat( actualAfter ).as( "Should have correct after layout" ).containsExactlyInAnyOrder( afterRecordsSizeX );
        }
    }

    private static class VersionUpdate extends FrekiCommand.Dispatcher.Adapter
    {
        private final boolean expectVersionChange;

        VersionUpdate( boolean expectVersionChange )
        {
            this.expectVersionChange = expectVersionChange;
        }

        @Override
        public void handle( FrekiCommand.SparseNode node )
        {
            byte chainVersion = StreamSupport.stream( node.spliterator(), false )
                    .map( r -> r.after ).filter( Objects::nonNull ).map( r -> r.version ).findFirst().orElse( Record.FIRST_VERSION );

            for ( FrekiCommand.RecordChange recordChange : node )
            {
                if ( recordChange.before != null && recordChange.after != null )
                {
                    if ( expectVersionChange )
                    {
                        assertThat( recordChange.after.version ).as( "Version expected to be updated" ).isNotEqualTo( recordChange.before.version );
                    }
                    else
                    {
                        assertThat( recordChange.after.version ).as( "Version expected to NOT be updated" ).isEqualTo( recordChange.before.version );
                    }
                }

                if ( recordChange.after != null )
                {
                    assertThat( recordChange.after.version ).as( "Chain should have have matching version" ).isEqualTo( (byte) chainVersion );
                }
            }
        }
    }

    private static class BigValueCounter extends FrekiCommand.Dispatcher.Adapter
    {
        final MutableInt numCreated = new MutableInt();
        final MutableInt numDeleted = new MutableInt();

        @Override
        public void handle( FrekiCommand.BigPropertyValue value )
        {
            for ( Record record : value.records )
            {
                (record.hasFlag( FLAG_IN_USE ) ? numCreated : numDeleted).increment();
            }
        }
    }
}
