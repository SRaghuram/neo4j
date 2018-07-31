/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.state;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.function.Predicates;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.impl.util.collection.CachingOffHeapBlockAllocator;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.impl.util.collection.OffHeapCollectionsFactory;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeWithPropertyValues;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.eclipse.collections.impl.factory.Sets.unionAll;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Pair.of;
import static org.neo4j.values.storable.ValueGroup.TEXT;
import static org.neo4j.values.storable.Values.NO_VALUE;

@SuppressWarnings( "unchecked" )
@RunWith( Parameterized.class )
public class TxStateTest
{
    private static final CachingOffHeapBlockAllocator BLOCK_ALLOCATOR = new CachingOffHeapBlockAllocator();

    public final RandomRule random = new RandomRule();

    @Rule
    public final TestRule repeatWithDifferentRandomization()
    {
        return RuleChain.outerRule( new RepeatRule() ).around( random );
    }

    private final IndexDescriptor indexOn_1_1 = TestIndexDescriptorFactory.forLabel( 1, 1 );
    private final IndexDescriptor indexOn_1_2 = TestIndexDescriptorFactory.forLabel( 1, 2 );
    private final IndexDescriptor indexOn_2_1 = TestIndexDescriptorFactory.forLabel( 2, 1 );

    private CollectionsFactory collectionsFactory;
    private TxState state;

    @Parameter
    public CollectionsFactorySupplier collectionsFactorySupplier;

    @Parameters( name = "{0}" )
    public static List<CollectionsFactorySupplier> data()
    {
        return asList( new CollectionsFactorySupplier()
        {
            @Override
            public CollectionsFactory create()
            {
                return CollectionsFactorySupplier.ON_HEAP.create();
            }

            @Override
            public String toString()
            {
                return "On heap";
            }
        }, new CollectionsFactorySupplier()
        {
            @Override
            public CollectionsFactory create()
            {
                return new OffHeapCollectionsFactory( BLOCK_ALLOCATOR );
            }

            @Override
            public String toString()
            {
                return "Off heap";
            }
        } );
    }

    @AfterClass
    public static void afterAll()
    {
        BLOCK_ALLOCATOR.release();
    }

    @Before
    public void before()
    {
        collectionsFactory = collectionsFactorySupplier.create();
        state = new TxState( collectionsFactory );
    }

    @After
    public void after()
    {
        collectionsFactory.release();
        assertEquals( "Seems like native memory is leaking", 0L, collectionsFactory.getMemoryTracker().usedDirectMemory() );
    }

    //region node label update tests

    @Test
    public void shouldGetAddedLabels()
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        LongSet addedLabels = state.nodeStateLabelDiffSets( 1 ).getAdded();

        // THEN
        assertEquals( newSetWith( 1, 2 ), addedLabels );
    }

    @Test
    public void shouldGetRemovedLabels()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        LongSet removedLabels = state.nodeStateLabelDiffSets( 1 ).getRemoved();

        // THEN
        assertEquals( newSetWith( 1, 2 ), removedLabels );
    }

    @Test
    public void removeAddedLabelShouldRemoveFromAdded()
    {
        // GIVEN
        state.nodeDoAddLabel( 1, 0 );
        state.nodeDoAddLabel( 1, 1 );
        state.nodeDoAddLabel( 2, 1 );

        // WHEN
        state.nodeDoRemoveLabel( 1, 1 );

        // THEN
        assertEquals( newSetWith( 2 ), state.nodeStateLabelDiffSets( 1 ).getAdded() );
    }

    @Test
    public void addRemovedLabelShouldRemoveFromRemoved()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 2, 1 );

        // WHEN
        state.nodeDoAddLabel( 1, 1 );

        // THEN
        assertEquals( newSetWith( 2 ), state.nodeStateLabelDiffSets( 1 ).getRemoved() );
    }

    @Test
    public void shouldMapFromRemovedLabelToNodes()
    {
        // GIVEN
        state.nodeDoRemoveLabel( 1, 0 );
        state.nodeDoRemoveLabel( 2, 0 );
        state.nodeDoRemoveLabel( 1, 1 );
        state.nodeDoRemoveLabel( 3, 1 );
        state.nodeDoRemoveLabel( 2, 2 );

        // WHEN
        LongSet nodes = state.nodesWithLabelChanged( 2 ).getRemoved();

        // THEN
        assertEquals( newSetWith( 0L, 2L ), nodes );
    }

    //endregion

    //region index rule tests

    @Test
    public void shouldAddAndGetByLabel()
    {
        // WHEN
        state.indexDoAdd( indexOn_1_1 );
        state.indexDoAdd( indexOn_2_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexDiffSetsByLabel( indexOn_1_1.schema().keyId() ).getAdded() );
    }

    @Test
    public void shouldAddAndGetByRuleId()
    {
        // GIVEN
        state.indexDoAdd( indexOn_1_1 );

        // THEN
        assertEquals( asSet( indexOn_1_1 ), state.indexChanges().getAdded() );
    }

    // endregion

    //region scan and seek index update tests

    @Test
    public void shouldComputeIndexUpdatesForScanOrSeekOnAnEmptyTxState()
    {
        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1 );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForScan( indexOn_1_1 );

        // THEN
        assertTrue( diffSets.isEmpty() );
        assertTrue( diffSets2.isEmpty() );
    }

    @Test
    public void shouldComputeIndexUpdatesForScanWhenThereAreNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1 );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForScan( indexOn_1_1 );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForSeekWhenThereAreNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForSeek( indexOn_1_1, ValueTuple.of( "value43" ) );

        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForScanWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForScan( indexOn_1_1 );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForScan( indexOn_1_1 );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForSeekWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForSeek( indexOn_1_1, ValueTuple.of( "value42" ) );

        // THEN
        assertEquals( newSetWith( 42L ), diffSets.getAdded() );
    }

    //endregion

    //region range seek by number index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 660 ), false, Values.of( 800 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 660 ), false, Values.of( 800 ), true );

        // THEN
        assertEquals( 0, diffSets.getAdded().size() );
        assertEquals( 0, diffSets2.getAdded().size() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInSingleBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 550 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 600 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 600 ), true );

        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 43L, 550 ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 42L, 500 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 44L, 520 ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( singletonList( of( 43L, 550 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 600 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 600 ), true );
        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 43L, 550 ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 550 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 550 ), true );

        // THEN
        assertEquals( newSetWith( 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ), newSetWithValues( 47L, 540 ),
                newSetWithValues( 48L, 550 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 550 ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), true, Values.of( 550 ), false );

        // THEN
        assertEquals( newSetWith( 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ), newSetWithValues( 47L, 540 ) ),
                diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), false, Values.of( 550 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), false, Values.of( 550 ), true );

        // THEN
        assertEquals( newSetWith( 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ), newSetWithValues( 47L, 540 ), newSetWithValues( 48L, 550 ) ),
                diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), false, Values.of( 550 ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 510 ), false, Values.of( 550 ), false );

        // THEN
        assertEquals( newSetWith( 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ), newSetWithValues( 47L, 540 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, false, Values.of( 550 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, false, Values.of( 550 ), true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, 500 ), newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ),
                newSetWithValues( 47L, 540 ), newSetWithValues( 48L, 550 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, Values.of( 550 ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, Values.of( 550 ), true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, 500 ), newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ),
                newSetWithValues( 47L, 540 ), newSetWithValues( 48L, 550 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, false, Values.of( 550 ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, false, Values.of( 550 ), false );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, 500 ), newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ),
                newSetWithValues( 47L, 540 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedLowerIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, Values.of( 550 ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, Values.of( 550 ), false );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, 500 ), newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ), newSetWithValues( 45L, 530 ),
                newSetWithValues( 47L, 540 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), true, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), true, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 47L, 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 47L, 540 ), newSetWithValues( 48L, 550 ), newSetWithValues( 49L, 560 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), true, NO_VALUE, false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), true, NO_VALUE, false );

        // THEN
        assertEquals( newSetWith( 47L, 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 47L, 540 ), newSetWithValues( 48L, 550 ), newSetWithValues( 49L, 560 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), false, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), false, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 48L, 550 ), newSetWithValues( 49L, 560 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithUnboundedUpperExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties(
                asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ), of( 45L, 530 ), of( 47L, 540 ), of( 48L, 550 ), of( 49L, 560 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), false, NO_VALUE, false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, Values.of( 540 ), false, NO_VALUE, false );

        // THEN
        assertEquals( newSetWith( 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 48L, 550 ), newSetWithValues( 49L, 560 ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByNumberWithNoBounds()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withNumberProperties( asList( of( 42L, 500 ), of( 43L, 510 ), of( 44L, 520 ) ) );
        addNodesToIndex( indexOn_1_2 ).withNumberProperties( singletonList( of( 46L, 520 ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, ValueGroup.NUMBER, NO_VALUE, true, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, 500 ), newSetWithValues( 43L, 510 ), newSetWithValues( 44L, 520 ) ), diffSets2.getAdded() );
    }

    //endregion

    //region range seek by string index update tests

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Barbara" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Cindy" ), false, Values.of( "William" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Cindy" ), false, Values.of( "William" ), true );

        // THEN
        assertEquals( 0, diffSets.getAdded().size() );
        assertEquals( 0, diffSets2.getAdded().size() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInSingleBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Barbara" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Cathy" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Cathy" ), true );

        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 43L, "Barbara" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties( singletonList( of( 42L, "Agatha" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 44L, "Andreas" ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties( singletonList( of( 43L, "Barbara" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Cathy" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Cathy" ), true );

        // THEN
        assertEquals( newSetWith( 43L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 43L, "Barbara" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Arwen" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Arwen" ), true );

        // THEN
        assertEquals( newSetWith( 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ), newSetWithValues( 45L, "Aristotle" ),
                newSetWithValues( 47L, "Arthur" ), newSetWithValues( 48L, "Arwen" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Arwen" ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), true, Values.of( "Arwen" ), false );

        // THEN
        assertEquals( newSetWith( 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ), newSetWithValues( 45L, "Aristotle" ),
                newSetWithValues( 47L, "Arthur" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), false, Values.of( "Arwen" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), false, Values.of( "Arwen" ), true );

        // THEN
        assertEquals( newSetWith( 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 44L, "Andreas" ), newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ),
                newSetWithValues( 48L, "Arwen" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), false, Values.of( "Arwen" ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Amy" ), false, Values.of( "Arwen" ), false );

        // THEN
        assertEquals( newSetWith( 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 44L, "Andreas" ), newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ) ),
                diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, false, Values.of( "Arwen" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, false, Values.of( "Arwen" ), true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Agatha" ), newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ),
                newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ), newSetWithValues( 48L, "Arwen" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, Values.of( "Arwen" ), true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, Values.of( "Arwen" ), true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L, 48L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Agatha" ), newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ),
                newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ), newSetWithValues( 48L, "Arwen" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, false, Values.of( "Arwen" ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, false, Values.of( "Arwen" ), false );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Agatha" ), newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ),
                newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedLowerIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, Values.of( "Arwen" ), false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, Values.of( "Arwen" ), false );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L, 45L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Agatha" ), newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ),
                newSetWithValues( 45L, "Aristotle" ), newSetWithValues( 47L, "Arthur" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), true, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), true, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 47L, 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 47L, "Arthur" ), newSetWithValues( 48L, "Arwen" ), newSetWithValues( 49L, "Ashley" ) ),
                diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperIncludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), true, NO_VALUE, false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), true, NO_VALUE, false );

        // THEN
        assertEquals( newSetWith( 47L, 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 47L, "Arthur" ), newSetWithValues( 48L, "Arwen" ), newSetWithValues( 49L, "Ashley" ) ),
                diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndIncludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), false, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), false, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 48L, "Arwen" ), newSetWithValues( 49L, "Ashley" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithUnboundedUpperExcludeLowerAndExcludeUpper()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ), of( 45L, "Aristotle" ), of( 47L, "Arthur" ), of( 48L, "Arwen" ),
                        of( 49L, "Ashley" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), false, NO_VALUE, false );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, Values.of( "Arthur" ), false, NO_VALUE, false );

        // THEN
        assertEquals( newSetWith( 48L, 49L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 48L, "Arwen" ), newSetWithValues( 49L, "Ashley" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForBetweenRangeSeekByStringWithNoBounds()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withBooleanProperties( asList( of( 39L, true ), of( 38L, false ) ) );
        addNodesToIndex( indexOn_1_1 ).withStringProperties( asList( of( 42L, "Agatha" ), of( 43L, "Amy" ), of( 44L, "Andreas" ) ) );
        addNodesToIndex( indexOn_1_2 ).withStringProperties( singletonList( of( 46L, "Andreas" ) ) );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, NO_VALUE, true );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeek( indexOn_1_1, TEXT, NO_VALUE, true, NO_VALUE, true );

        // THEN
        assertEquals( newSetWith( 42L, 43L, 44L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Agatha" ), newSetWithValues( 43L, "Amy" ), newSetWithValues( 44L, "Andreas" ) ), diffSets2.getAdded() );
    }

    //endregion

    //region range seek by suffix or contains index update tests

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets =
                state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "eulav" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "eulav" ) );

        // THEN
        assertEquals( 0, diffSets.getAdded().size() );
        assertEquals( 0, diffSets2.getAdded().size() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereAreNewNodesCreatedInOneBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets =
                state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "value" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "value" ) );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekBySuffixWhenThereArePartiallyMatchingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringSuffix( indexOn_1_1.schema().getPropertyId(), "ella" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringSuffix( indexOn_1_1.schema().getPropertyId(), "ella" ) );

        // THEN
        assertEquals( newSetWith( 46L, 47L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 46L, "Barbarella" ), newSetWithValues( 47L, "Cinderella" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereArePartiallyMatchingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets =
                state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "arbar" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "arbar" ) );

        // THEN
        assertEquals( newSetWith( 45L, 46L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 45L, "Barbara" ), newSetWithValues( 46L, "Barbarella" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekBySuffixWhenThereArePartiallyMatchingLeadingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringSuffix( indexOn_1_1.schema().getPropertyId(), "ron" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringSuffix( indexOn_1_1.schema().getPropertyId(), "ron" ) );

        // THEN
        assertEquals( newSetWith( 40L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 40L, "Aaron" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereArePartiallyMatchingTrailingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets =
                state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "inder" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "inder" ) );

        // THEN
        assertEquals( newSetWith( 47L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 47L, "Cinderella" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByContainsWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        LongDiffSets diffSets =
                state.indexUpdatesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "value4" ) );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 =
                state.indexUpdatesWithValuesForSuffixOrContains( indexOn_1_1, IndexQuery.stringContains( indexOn_1_1.schema().getPropertyId(), "value4" ) );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    //endregion

    //region range seek by prefix index update tests

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNoMatchingNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "eulav" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "eulav" );

        // THEN
        assertEquals( 0, diffSets.getAdded().size() );
        assertEquals( 0, diffSets2.getAdded().size() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInOneBatch()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L, 43L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes1()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "And" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "And" );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "Andreas" ), newSetWithValues( 43L, "Andrea" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingNewNodes2()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Bar" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "Bar" );

        // THEN
        assertEquals( newSetWith( 45L, 46L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 45L, "Barbara" ), newSetWithValues( 46L, "Barbarella" ) ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingLeadingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Aa" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "Aa" );

        // THEN
        assertEquals( newSetWith( 40L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 40L, "Aaron" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereArePartiallyMatchingTrailingNewNodes()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withStringProperties(
                asList( of( 40L, "Aaron" ), of( 41L, "Agatha" ), of( 42L, "Andreas" ), of( 43L, "Andrea" ), of( 44L, "Aristotle" ), of( 45L, "Barbara" ),
                        of( 46L, "Barbarella" ), of( 47L, "Cinderella" ) ) );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "Ci" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "Ci" );

        // THEN
        assertEquals( newSetWith( 47L ), diffSets.getAdded() );
        assertEquals( newSetWithValues( 47L, "Cinderella" ), diffSets2.getAdded() );
    }

    @Test
    public void shouldComputeIndexUpdatesForRangeSeekByPrefixWhenThereAreNewNodesCreatedInTwoBatches()
    {
        // GIVEN
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 42L );
        addNodesToIndex( indexOn_1_2 ).withDefaultStringProperties( 44L );
        addNodesToIndex( indexOn_1_1 ).withDefaultStringProperties( 43L );

        // WHEN
        LongDiffSets diffSets = state.indexUpdatesForRangeSeekByPrefix( indexOn_1_1, "value" );
        ReadableDiffSets<NodeWithPropertyValues> diffSets2 = state.indexUpdatesWithValuesForRangeSeekByPrefix( indexOn_1_1, "value" );

        // THEN
        assertEquals( newSetWith( 42L, 43L ), diffSets.getAdded() );
        assertEquals( unionAll( newSetWithValues( 42L, "value42" ), newSetWithValues( 43L, "value43" ) ), diffSets2.getAdded() );
    }

    //endregion

    //region miscellaneous

    @Test
    public void shouldListNodeAsDeletedIfItIsDeleted()
    {
        // Given

        // When
        long nodeId = 1337L;
        state.nodeDoDelete( nodeId );

        // Then
        assertThat( state.addedAndRemovedNodes().getRemoved(), equalTo( newSetWith( nodeId ) ) );
    }

    @Test
    public void shouldAddUniquenessConstraint()
    {
        // when
        UniquenessConstraintDescriptor constraint = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint, 7 );

        // then
        ReadableDiffSets<ConstraintDescriptor> diff = state.constraintsChangesForLabel( 1 );

        assertEquals( singleton( constraint ), diff.getAdded() );
        assertTrue( diff.getRemoved().isEmpty() );
    }

    @Test
    public void addingUniquenessConstraintShouldBeIdempotent()
    {
        // given
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );

        // when
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
    }

    @Test
    public void shouldDifferentiateBetweenUniquenessConstraintsForDifferentLabels()
    {
        // when
        UniquenessConstraintDescriptor constraint1 = ConstraintDescriptorFactory.uniqueForLabel( 1, 17 );
        state.constraintDoAdd( constraint1, 7 );
        UniquenessConstraintDescriptor constraint2 = ConstraintDescriptorFactory.uniqueForLabel( 2, 17 );
        state.constraintDoAdd( constraint2, 19 );

        // then
        assertEquals( singleton( constraint1 ), state.constraintsChangesForLabel( 1 ).getAdded() );
        assertEquals( singleton( constraint2 ), state.constraintsChangesForLabel( 2 ).getAdded() );
    }

    @Test
    public void shouldAddRelationshipPropertyExistenceConstraint()
    {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( 1, 42 );

        // When
        state.constraintDoAdd( constraint );

        // Then
        assertEquals( singleton( constraint ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void addingRelationshipPropertyExistenceConstraintConstraintShouldBeIdempotent()
    {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType( 1, 42 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType( 1, 42 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );

        // Then
        assertEquals( constraint1, constraint2 );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
    }

    @Test
    public void shouldDropRelationshipPropertyExistenceConstraint()
    {
        // Given
        ConstraintDescriptor constraint = ConstraintDescriptorFactory.existsForRelType( 1, 42 );
        state.constraintDoAdd( constraint );

        // When
        state.constraintDoDrop( constraint );

        // Then
        assertTrue( state.constraintsChangesForRelationshipType( 1 ).isEmpty() );
    }

    @Test
    public void shouldDifferentiateRelationshipPropertyExistenceConstraints()
    {
        // Given
        ConstraintDescriptor constraint1 = ConstraintDescriptorFactory.existsForRelType( 1, 11 );
        ConstraintDescriptor constraint2 = ConstraintDescriptorFactory.existsForRelType( 1, 22 );
        ConstraintDescriptor constraint3 = ConstraintDescriptorFactory.existsForRelType( 3, 33 );

        // When
        state.constraintDoAdd( constraint1 );
        state.constraintDoAdd( constraint2 );
        state.constraintDoAdd( constraint3 );

        // Then
        assertEquals( asSet( constraint1, constraint2 ), state.constraintsChangesForRelationshipType( 1 ).getAdded() );
        assertEquals( singleton( constraint1 ), state.constraintsChangesForSchema( constraint1.schema() ).getAdded() );
        assertEquals( singleton( constraint2 ), state.constraintsChangesForSchema( constraint2.schema() ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForRelationshipType( 3 ).getAdded() );
        assertEquals( singleton( constraint3 ), state.constraintsChangesForSchema( constraint3.schema() ).getAdded() );
    }

    @Test
    public void shouldListRelationshipsAsCreatedIfCreated()
    {
        // When
        long relId = 10;
        state.relationshipDoCreate( relId, 0, 1, 2 );

        // Then
        assertTrue( state.hasChanges() );
        assertTrue( state.relationshipIsAddedInThisTx( relId ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedNode() throws Exception
    {
        // GIVEN
        state.nodeDoCreate( 0 );
        state.nodeDoDelete( 0 );
        state.nodeDoCreate( 1 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedNode( long id )
            {
                assertEquals( "Should not create any other node than 1", 1, id );
            }

            @Override
            public void visitDeletedNode( long id )
            {
                fail( "Should not delete any node" );
            }
        } );
    }

    @Test
    public void shouldVisitDeletedNode() throws Exception
    {
        // Given
        state.nodeDoDelete( 42 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedNode( long id )
            {
                // Then
                assertEquals( "Wrong deleted node id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedNodeIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );
        state.nodeDoDelete( nodeId );

        // Then
        assertTrue( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotReportDeletedNodeIfItIsNotDeleted()
    {
        // Given
        long nodeId = 42;

        // When
        state.nodeDoCreate( nodeId );

        // Then
        assertFalse( state.nodeIsDeletedInThisTx( nodeId ) );
    }

    @Test
    public void shouldNotChangeRecordForCreatedAndDeletedRelationship() throws Exception
    {
        // GIVEN
        state.relationshipDoCreate( 0, 0, 1, 2 );
        state.relationshipDoDelete( 0, 0, 1, 2 );
        state.relationshipDoCreate( 1, 0, 2, 3 );

        // WHEN
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                assertEquals( "Should not create any other relationship than 1", 1, id );
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                fail( "Should not delete any relationship" );
            }
        } );
    }

    @Test
    public void doNotVisitNotModifiedPropertiesOnModifiedNodes() throws ConstraintValidationException, CreateConstraintFailureException
    {
        state.nodeDoAddLabel( 5, 1 );
        MutableBoolean labelsChecked = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
            {
                labelsChecked.setTrue();
                assertEquals( 1, id );
                assertEquals( 1, added.size() );
                assertTrue( added.contains( 5 ) );
                assertTrue( removed.isEmpty() );
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            {
                fail( "Properties were not changed." );
            }
        } );
        assertTrue( labelsChecked.booleanValue() );
    }

    @Test
    public void doNotVisitNotModifiedLabelsOnModifiedNodes() throws ConstraintValidationException, CreateConstraintFailureException
    {
        state.nodeDoAddProperty( 1, 2, Values.stringValue( "propertyValue" ) );
        MutableBoolean propertiesChecked = new MutableBoolean();
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
            {
                fail( "Labels were not changed." );
            }

            @Override
            public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
            {
                propertiesChecked.setTrue();
                assertEquals( 1, id );
                assertFalse( changed.hasNext() );
                assertTrue( removed.isEmpty() );
                assertEquals( 1, Iterators.count( added, Predicates.alwaysTrue() ) );
            }
        } );
        assertTrue( propertiesChecked.booleanValue() );
    }

    @Test
    public void shouldVisitDeletedRelationship() throws Exception
    {
        // Given
        state.relationshipDoDelete( 42, 2, 3, 4 );

        // When
        state.accept( new TxStateVisitor.Adapter()
        {
            @Override
            public void visitDeletedRelationship( long id )
            {
                // Then
                assertEquals( "Wrong deleted relationship id", 42, id );
            }
        } );
    }

    @Test
    public void shouldReportDeletedRelationshipIfItWasCreatedAndDeletedInSameTx()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );
        state.relationshipDoDelete( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertTrue( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    public void shouldNotReportDeletedRelationshipIfItIsNotDeleted()
    {
        // Given
        long startNodeId = 1;
        long relationshipId = 2;
        int relationshipType = 3;
        long endNodeId = 4;

        // When
        state.relationshipDoCreate( relationshipId, relationshipType, startNodeId, endNodeId );

        // Then
        assertFalse( state.relationshipIsDeletedInThisTx( relationshipId ) );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedNodesBeforeDeletedNodes() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedNodesBeforeCreatedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.nodeDoCreate( /*id=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitCreatedNode( long id )
            {
                visitEarly();
            }

            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitCreatedRelationshipsBeforeDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.relationshipDoDelete( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            // then
            @Override
            public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitLate();
            }
        } );
    }

    @Test
    @RepeatRule.Repeat( times = 100 )
    public void shouldVisitDeletedNodesAfterDeletedRelationships() throws Exception
    {
        // when
        state.accept( new VisitationOrder( random.nextInt( 100 ) )
        {
            // given

            @Override
            void createEarlyState()
            {
                state.relationshipDoCreate( /*id=*/random.nextInt( 1 << 20 ),
                        /*type=*/random.nextInt( 128 ),
                        /*startNode=*/random.nextInt( 1 << 20 ),
                        /*endNode=*/random.nextInt( 1 << 20 ) );
            }

            @Override
            void createLateState()
            {
                state.nodeDoDelete( /*id=*/random.nextInt( 1 << 20 ) );
            }

            // then

            @Override
            public void visitDeletedRelationship( long id )
            {
                visitEarly();
            }

            @Override
            public void visitDeletedNode( long id )
            {
                visitLate();
            }
        } );
    }

    //endregion

    static Set<NodeWithPropertyValues> newSetWithValues( long nodeId, Object... values )
    {
        return singleton( new NodeWithPropertyValues( nodeId, Arrays.stream( values ).map( ValueUtils::of ).toArray( Value[]::new ) ) );
    }

    static Set<NodeWithPropertyValues> newSetWithValues( long nodeId, Value... values )
    {
        return singleton( new NodeWithPropertyValues( nodeId, values ) );
    }

    abstract class VisitationOrder extends TxStateVisitor.Adapter
    {
        private final Set<String> visitMethods = new HashSet<>();

        VisitationOrder( int size )
        {
            for ( Method method : getClass().getDeclaredMethods() )
            {
                if ( method.getName().startsWith( "visit" ) )
                {
                    visitMethods.add( method.getName() );
                }
            }
            assertEquals( "should implement exactly two visit*(...) methods", 2, visitMethods.size() );
            do
            {
                if ( random.nextBoolean() )
                {
                    createEarlyState();
                }
                else
                {
                    createLateState();
                }
            }
            while ( size-- > 0 );
        }

        abstract void createEarlyState();

        abstract void createLateState();

        private boolean late;

        final void visitEarly()
        {
            if ( late )
            {
                String early = "the early visit*-method";
                String late = "the late visit*-method";
                for ( StackTraceElement trace : Thread.currentThread().getStackTrace() )
                {
                    if ( visitMethods.contains( trace.getMethodName() ) )
                    {
                        early = trace.getMethodName();
                        for ( String method : visitMethods )
                        {
                            if ( !method.equals( early ) )
                            {
                                late = method;
                            }
                        }
                        break;
                    }
                }
                fail( early + "(...) should not be invoked after " + late + "(...)" );
            }
        }

        final void visitLate()
        {
            late = true;
        }
    }

    private interface IndexUpdater
    {
        void withDefaultStringProperties( long... nodeIds );

        void withStringProperties( Collection<Pair<Long,String>> nodesWithValues );

        <T extends Number> void withNumberProperties( Collection<Pair<Long,T>> nodesWithValues );

        void withBooleanProperties( Collection<Pair<Long,Boolean>> nodesWithValues );
    }

    private IndexUpdater addNodesToIndex( final IndexDescriptor descriptor )
    {
        return new IndexUpdater()
        {
            @Override
            public void withDefaultStringProperties( long... nodeIds )
            {
                Collection<Pair<Long,String>> entries = new ArrayList<>( nodeIds.length );
                for ( long nodeId : nodeIds )
                {
                    entries.add( of( nodeId, "value" + nodeId ) );
                }
                withStringProperties( entries );
            }

            @Override
            public void withStringProperties( Collection<Pair<Long,String>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            @Override
            public <T extends Number> void withNumberProperties( Collection<Pair<Long,T>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            @Override
            public void withBooleanProperties( Collection<Pair<Long,Boolean>> nodesWithValues )
            {
                withProperties( nodesWithValues );
            }

            private <T> void withProperties( Collection<Pair<Long,T>> nodesWithValues )
            {
                final int labelId = descriptor.schema().keyId();
                final int propertyKeyId = descriptor.schema().getPropertyId();
                for ( Pair<Long,T> entry : nodesWithValues )
                {
                    long nodeId = entry.first();
                    state.nodeDoCreate( nodeId );
                    state.nodeDoAddLabel( labelId, nodeId );
                    Value valueAfter = Values.of( entry.other() );
                    state.nodeDoAddProperty( nodeId, propertyKeyId, valueAfter );
                    state.indexDoUpdateEntry( descriptor.schema(), nodeId, null, ValueTuple.of( valueAfter ) );
                }
            }
        };
    }
}
