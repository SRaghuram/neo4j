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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.txstate.LongDiffSets;
import org.neo4j.storageengine.api.txstate.NodeState;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Integer.min;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * Focus of this test is to verify that creating and applying commands via {@link FrekiStorageEngine} surface works.
 */
@PageCacheExtension
@ExtendWith( RandomExtension.class )
class FrekiStorageEngineGraphWritesIT
{
    private static final SchemaDescriptor SCHEMA_DESCRIPTOR = SchemaDescriptor.forLabel( 5, 10 );
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    private LifeSupport life;
    private FrekiStorageEngine storageEngine;
    private RecordingNodeLabelUpdateListener nodeLabelUpdateListener;
    private RecordingIndexUpdatesListener indexUpdateListener;
    private CommandCreationContext commandCreationContext;

    @BeforeEach
    void start() throws IOException
    {
        DatabaseLayout layout = Neo4jLayout.of( directory.homeDir() ).databaseLayout( DEFAULT_DATABASE_NAME );
        fs.mkdirs( layout.databaseDirectory() );
        TokenHolders tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        DatabaseHealth databaseHealth = new DatabaseHealth( new DatabasePanicEventGenerator(
                new DatabaseEventListeners( NullLog.getInstance() ), DEFAULT_DATABASE_NAME ), NullLog.getInstance() );
        life = new LifeSupport();
        MemoryTracker memoryTracker = EmptyMemoryTracker.INSTANCE;
        storageEngine = life.add( new FrekiStorageEngine(
                fs, layout, Config.defaults(), pageCache, tokenHolders, mock( SchemaState.class ), new StandardConstraintRuleAccessor(),
                        new NoopIndexConfigCompletor(), NO_LOCK_SERVICE, new DefaultIdGeneratorFactory( fs, RecoveryCleanupWorkCollector.immediate() ),
                        new DefaultIdController(), databaseHealth, NullLogProvider.getInstance(), RecoveryCleanupWorkCollector.immediate(),
                        true, PageCacheTracer.NULL, CursorAccessPatternTracer.NO_TRACING, memoryTracker ) );
        nodeLabelUpdateListener = new RecordingNodeLabelUpdateListener();
        storageEngine.addNodeLabelUpdateListener( nodeLabelUpdateListener );
        indexUpdateListener = new RecordingIndexUpdatesListener();
        storageEngine.addIndexUpdateListener( indexUpdateListener );
        life.start();
        commandCreationContext = storageEngine.newCommandCreationContext( NULL, memoryTracker );
    }

    @AfterEach
    void stop()
    {
        life.shutdown();
    }

    @Test
    void shouldCreateNodeWithLabelsPropertiesAndRelationships() throws Exception
    {
        // given
        long nodeId1 = 1;
        long nodeId2 = 2;
        LongSet labelIds = LongSets.immutable.of( 1, 3, 6 );
        Set<StorageProperty> nodeProperties = asSet( new PropertyKeyValue( 1, intValue( 101 ) ), new PropertyKeyValue( 2, stringValue( "abc" ) ) );
        Set<StorageProperty> relationshipProperties = asSet( new PropertyKeyValue( 5, intValue( 202 ) ) );
        Set<RelationshipSpec> relationships = asSet( new RelationshipSpec( nodeId1, 99, nodeId2, relationshipProperties, commandCreationContext ) );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId1 );
            target.visitNodeLabelChanges( nodeId1, labelIds, LongSets.immutable.empty() );
            target.visitCreatedNode( nodeId2 );
            target.visitNodePropertyChanges( nodeId1, nodeProperties, Collections.emptyList(), IntSets.immutable.empty() );
            relationships.forEach( r -> r.create( target ) );
        } );

        // then
        assertContentsOfNode( nodeId1, labelIds, nodeProperties, relationships );
    }

    @Test
    void shouldAddDataToExistingNode() throws Exception
    {
        // given
        long nodeId1 = 1;
        long nodeId2 = 2;
        MutableLongSet labelIds = LongSets.mutable.of( 1 );
        Set<StorageProperty> nodeProperties = asSet( new PropertyKeyValue( 1, intValue( 101 ) ) );
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId1 );
            target.visitCreatedNode( nodeId2 );
            target.visitNodeLabelChanges( nodeId1, labelIds, LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId1, nodeProperties, Collections.emptyList(), IntSets.immutable.empty() );
        } );
        assertContentsOfNode( nodeId1, labelIds, nodeProperties, emptySet() );

        // when
        Set<StorageProperty> relationshipProperties = asSet( new PropertyKeyValue( 5, intValue( 202 ) ) );
        Set<RelationshipSpec> relationships = asSet( new RelationshipSpec( nodeId1, 99, nodeId2, relationshipProperties, commandCreationContext ) );
        createAndApplyTransaction( target ->
        {
            LongSet addedLabels = LongSets.immutable.of( 3, 6 );
            labelIds.addAll( addedLabels );
            target.visitNodeLabelChanges( nodeId1, addedLabels, LongSets.immutable.empty() );
            Set<StorageProperty> addedNodeProperties = asSet( new PropertyKeyValue( 2, longValue( 202 ) ) );
            target.visitNodePropertyChanges( nodeId1, addedNodeProperties, Collections.emptyList(), IntSets.immutable.empty() );
            nodeProperties.addAll( addedNodeProperties );
            relationships.forEach( r -> r.create( target ) );
        } );

        // then
        assertContentsOfNode( nodeId1, labelIds, nodeProperties, relationships );
    }

    @Test
    void shouldOverflowIntoLargerRecord() throws Exception
    {
        // given
        long nodeId = 10;
        LongSet labelIds = LongSets.immutable.of( 34, 563 );
        Set<StorageProperty> nodeProperties = asSet(
                new PropertyKeyValue( 1, intValue( 101 ) ),
                new PropertyKeyValue( 2, longValue( 101101 ) ),
                new PropertyKeyValue( 3, doubleValue( 101.101 ) ),
                new PropertyKeyValue( 7, stringValue( "abcdef" ) ) );
        Set<RelationshipSpec> relationships = new HashSet<>();

        // when
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labelIds, LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, nodeProperties, emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < 30; i++ )
            {
                long otherNodeId = nodeId + i + 1;
                int type = i % 3;
                target.visitCreatedNode( otherNodeId );
                relationships.add( createRelationship( target, commandCreationContext, nodeId, type, otherNodeId, emptySet() ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labelIds, nodeProperties, relationships );
    }

    @Test
    void shouldUpdateOverflowedRecord() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        LongSet labelIds = LongSets.immutable.of( 34, 563 );
        Set<StorageProperty> nodeProperties = asSet(
                new PropertyKeyValue( 1, intValue( 101 ) ),
                new PropertyKeyValue( 2, longValue( 101101 ) ),
                new PropertyKeyValue( 3, doubleValue( 101.101 ) ),
                new PropertyKeyValue( 7, stringValue( "abcdef" ) ) );
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labelIds, LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, nodeProperties, emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < 20; i++ )
            {
                long otherNodeId = commandCreationContext.reserveNode();
                int type = i % 3;
                target.visitCreatedNode( otherNodeId );
                relationships.add( createRelationship( target, commandCreationContext, nodeId, type, otherNodeId, emptySet() ) );
            }
        } );

        // when
        createAndApplyTransaction( target ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                long otherNodeId = commandCreationContext.reserveNode();
                int type = i % 3;
                target.visitCreatedNode( otherNodeId );
                relationships.add( createRelationship( target, commandCreationContext, nodeId, type, otherNodeId, emptySet() ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labelIds, nodeProperties, relationships );
    }

    @Test
    void shouldOverflowIntoDenseRepresentation() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        ImmutableLongSet labels = LongSets.immutable.of( 1, 78, 95 );
        Set<StorageProperty> relationshipProperties = asSet( new PropertyKeyValue( 123, intValue( 5 ) ) );
        Set<StorageProperty> nodeProperties = new HashSet<>();

        // when
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
            for ( int i = 0; i < 20; i++ )
            {
                nodeProperties.add( new PropertyKeyValue( i, longValue( 1L << i ) ) );
            }
            target.visitNodePropertyChanges( nodeId, nodeProperties, emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < 1_000; i++ )
            {
                long otherNodeId = commandCreationContext.reserveNode();
                target.visitCreatedNode( otherNodeId );
                int type = i % 20;
                relationships.add( createRelationship( target, commandCreationContext, nodeId, type, otherNodeId, relationshipProperties ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labels, nodeProperties, relationships );
    }

    @Test
    void shouldAllowLabelsInXL() throws Exception
    {
        long nodeId = commandCreationContext.reserveNode();
        MutableLongSet labels = LongSets.mutable.empty();
        for ( int i = 0; i < 200; i++ ) //should not fit in x1
        {
            labels.add( i * 5 );
        }
        Set<StorageProperty> nodeProperties = new HashSet<>();
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
        } );

        assertContentsOfNode( nodeId, labels, nodeProperties, relationships );
    }

    @Test
    void shouldHaveIndexUpdatesWhenLabelsAreInXL() throws Exception
    {
        long nodeId = commandCreationContext.reserveNode();
        MutableLongSet labels = LongSets.mutable.empty();
        for ( int i = 0; i < 200; i++ ) //should not fit in x1
        {
            labels.add( i * 5 );
        }
        labels.add( SCHEMA_DESCRIPTOR.getLabelId() );

        IntValue afterValue = Values.intValue( 77 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
        }, target -> target.visitNodePropertyChanges( nodeId, emptyList(), singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                IntSets.immutable.empty() ), index -> asSet( IndexEntryUpdate.add( nodeId, index, afterValue ) ) );
    }

    @Test
    void shouldOverflowIntoLargerLargerAndDense() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        ImmutableLongSet labels = LongSets.immutable.of( 1, 78, 95 );
        Set<StorageProperty> nodeProperties = new HashSet<>();
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
        } );

        // when
        MutableLong nextOtherNodeId = new MutableLong( nodeId + 1 );
        MutableInt nextPropertyKey = new MutableInt();
        for ( int i = 0, relationshipsToAdd = 5; i < 5; i++, relationshipsToAdd *= 4 )
        {
            int relationshipCount = relationshipsToAdd;
            createAndApplyTransaction( target ->
            {
                for ( int r = 0; r < relationshipCount; r++ )
                {
                    long otherNodeId = nextOtherNodeId.getAndIncrement();
                    target.visitCreatedNode( otherNodeId );
                    relationships.add( createRelationship( target, commandCreationContext, nodeId, 0, otherNodeId, emptySet() ) );
                }
                PropertyKeyValue property = new PropertyKeyValue( nextPropertyKey.getAndIncrement(), intValue( 1010 ) );
                target.visitNodePropertyChanges( nodeId, singletonList( property ), emptyList(), IntSets.immutable.empty() );
                nodeProperties.add( property );
            } );

            // then
            assertContentsOfNode( nodeId, labels, nodeProperties, relationships );
        }
    }

    @Test
    void shouldOverwriteRecordMultipleTimes() throws Exception
    {
        // given
        long[] nodes = new long[10];
        createAndApplyTransaction( target ->
        {
            for ( int i = 0; i < nodes.length; i++ )
            {
                target.visitCreatedNode( nodes[i] = commandCreationContext.reserveNode() );
            }
        } );

        // when
        MutableLongObjectMap<Set<RelationshipSpec>> expectedRelationships = LongObjectMaps.mutable.empty();
        for ( int i = 0; i < 10; i++ )
        {
            createAndApplyTransaction( target ->
            {
                for ( int j = 0; j < nodes.length || expectedRelationships.size() < nodes.length; j++ )
                {
                    long startNode = nodes[random.nextInt( nodes.length )];
                    long endNode = nodes[random.nextInt( nodes.length )];
                    RelationshipSpec relationship = createRelationship( target, commandCreationContext, startNode, random.nextInt( 4 ), endNode, emptySet() );
                    expectedRelationships.getIfAbsentPut( startNode, HashSet::new ).add( relationship );
                    expectedRelationships.getIfAbsentPut( endNode, HashSet::new ).add( relationship );
                }
            } );

            // then
            for ( long node : nodes )
            {
                assertContentsOfNode( node, LongSets.immutable.empty(), emptySet(), expectedRelationships.get( node ) );
            }
        }
    }

    @Test
    void shouldDeleteRelationshipFromSparseNode() throws Exception
    {
        shouldDeleteRelationship( 2, 20 );
    }

    @Test
    void shouldDeleteRelationshipFromDenseNode() throws Exception
    {
        shouldDeleteRelationship( 100, 200 );
    }

    private void shouldDeleteRelationship( int atLeastNumRelationships, int atMostNumRelationships ) throws Exception
    {
        // given
        long nodeId1 = commandCreationContext.reserveNode();
        long nodeId2 = commandCreationContext.reserveNode();
        List<RelationshipSpec> relationships = new ArrayList<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId1 );
            target.visitCreatedNode( nodeId2 );
            int numberOfRelationships = random.nextInt( atLeastNumRelationships, atMostNumRelationships );
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                boolean outgoing = random.nextBoolean();
                relationships.add( createRelationship( target, commandCreationContext, outgoing ? nodeId1 : nodeId2, random.nextInt( 3 ),
                        outgoing ? nodeId2 : nodeId1, asSet( new PropertyKeyValue( 0, intValue( i ) ) ) ) );
            }
        } );

        // when
        createAndApplyTransaction( target ->
        {
            RelationshipSpec deletedRelationship = relationships.remove( random.nextInt( relationships.size() ) );
            target.visitDeletedRelationship( deletedRelationship.id, deletedRelationship.type, deletedRelationship.startNodeId,
                    deletedRelationship.endNodeId );
        } );

        // then
        assertContentsOfNode( nodeId1, LongSets.immutable.empty(), emptySet(), new HashSet<>( relationships ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnSmallCreatedNode() throws Exception
    {
        long nodeId = 123;
        Value value = intValue( 98765 );
        shouldGenerateIndexUpdates( null, target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.add( nodeId, index, value ) ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnSmallUpdatedNode_Update() throws Exception
    {
        long nodeId = 123;
        Value beforeValue = intValue( 98765 );
        Value afterValue = intValue( 56789 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), beforeValue ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, target -> target.visitNodePropertyChanges( nodeId, emptyList(), singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                IntSets.immutable.empty() ), index -> asSet( IndexEntryUpdate.change( nodeId, index, new Value[]{beforeValue}, new Value[]{afterValue} ) ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnSmallUpdatedNode_Remove() throws Exception
    {
        long nodeId = 123;
        Value value = intValue( 98765 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, target ->
        {
            target.visitNodePropertyChanges( nodeId, emptyList(), emptyList(), IntSets.immutable.of( SCHEMA_DESCRIPTOR.getPropertyId() ) );
        }, index -> asSet( IndexEntryUpdate.remove( nodeId, index, value ) ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnSmallNodeRemoved() throws Exception
    {
        long nodeId = 123;
        Value value = intValue( 98765 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, target ->
        {
            target.visitDeletedNode( nodeId );
        }, index -> asSet( IndexEntryUpdate.remove( nodeId, index, value ) ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnSmallNodeOverflowingToLarge() throws Exception
    {
        long nodeId = 123;
        Value beforeValue = intValue( 98765 );
        Value afterValue = intValue( 56789 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), beforeValue ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, target ->
        {
            List<StorageProperty> addedProperties = new ArrayList<>();
            for ( int i = 0; i < 5; i++ )
            {
                addedProperties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + 1 + i, stringValue( "string-" + i ) ) );
            }
            target.visitNodePropertyChanges( nodeId, addedProperties, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.change( nodeId, index, new Value[]{beforeValue}, new Value[]{afterValue} ) ) );
    }

    @Test
    void shouldGenerateIndexUpdatesOnLargeNodeOverflowingToLarger() throws Exception
    {
        long nodeId = 123;
        Value beforeValue = intValue( 98765 );
        Value afterValue = intValue( 56789 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            List<StorageProperty> addedProperties = new ArrayList<>();
            for ( int i = 0; i < 5; i++ )
            {
                addedProperties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + 1 + i, stringValue( "string-" + i ) ) );
            }
            addedProperties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), beforeValue ) );
            target.visitNodePropertyChanges( nodeId, addedProperties, emptyList(), IntSets.immutable.empty() );
        }, target ->
        {
            List<StorageProperty> addedProperties = new ArrayList<>();
            for ( int i = 0; i < 5; i++ )
            {
                addedProperties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + 100 + i, stringValue( "string-" + i ) ) );
            }
            target.visitNodePropertyChanges( nodeId, addedProperties, singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.change( nodeId, index, new Value[]{beforeValue}, new Value[]{afterValue} ) ) );
    }

    @Test
    void shouldGenerateAddedIndexUpdateOnNodeOverflowingToDense() throws Exception
    {
        long nodeId = 123;
        Value value = intValue( 98765 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
        }, target ->
        {
            // Just make the node dense by adding lots of relationships to it
            for ( int i = 0; i < 200; i++ )
            {
                long otherNodeId = nodeId + i + 1;
                target.visitCreatedNode( otherNodeId );
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, otherNodeId, emptyList() );
            }
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.add( nodeId, index, value ) ) );
    }

    @Test
    void shouldGenerateUpdatedIndexUpdateOnNodeOverflowingToDense() throws Exception
    {
        long nodeId = 123;
        Value beforeValue = intValue( 98765 );
        Value afterValue = intValue( 56789 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), beforeValue ) ),
                    emptyList(), IntSets.immutable.empty() );
        }, target ->
        {
            // Just make the node dense by adding lots of relationships to it
            for ( int i = 0; i < 200; i++ )
            {
                long otherNodeId = nodeId + i + 1;
                target.visitCreatedNode( otherNodeId );
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, otherNodeId, emptyList() );
            }
            target.visitNodePropertyChanges( nodeId, emptyList(), singletonList( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.change( nodeId, index, new Value[]{beforeValue}, new Value[]{afterValue} ) ) );
    }

    @Test
    void shouldGenerateAddedIndexUpdateOnAddingToAlreadyDenseNode() throws Exception
    {
        long nodeId = 123;
        Value value = intValue( 98765 );
        shouldGenerateIndexUpdates( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
            // Just make the node dense by adding lots of relationships to it
            for ( int i = 0; i < 200; i++ )
            {
                long otherNodeId = nodeId + i + 1;
                target.visitCreatedNode( otherNodeId );
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, otherNodeId, emptyList() );
            }
        }, target ->
        {
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.add( nodeId, index, value ) ) );
    }

    @Test
    void shouldDeleteSparseNode() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( 1, 2, 3 ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, asList( new PropertyKeyValue( 0, intValue( 10 ) ), new PropertyKeyValue( 1, stringValue( "abc" ) ) ),
                    emptyList(), IntSets.immutable.empty() );
        } );

        // when
        createAndApplyTransaction( target -> target.visitDeletedNode( nodeId ) );

        // then
        try ( StorageReader reader = storageEngine.newReader();
              StorageNodeCursor nodeCursor = reader.allocateNodeCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isFalse();
        }
    }

    @Test
    void shouldDeleteDenseNode() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( 1, 2, 3 ), LongSets.immutable.empty() );
            target.visitNodePropertyChanges( nodeId, asList( new PropertyKeyValue( 0, intValue( 10 ) ), new PropertyKeyValue( 1, stringValue( "abc" ) ) ),
                    emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < 100; i++ )
            {
                long otherNode = commandCreationContext.reserveNode();
                target.visitCreatedNode( otherNode );
                Set<StorageProperty> properties = asSet( new PropertyKeyValue( 0, intValue( i ) ) );
                relationships.add( createRelationship( target, commandCreationContext, nodeId, 0, otherNode, properties ) );
            }
        } );

        // when
        createAndApplyTransaction( target ->
        {
            relationships.forEach( r -> target.visitDeletedRelationship( r.id, r.type, r.startNodeId, r.endNodeId ) );
            target.visitDeletedNode( nodeId );
        } );

        // then
        try ( StorageReader reader = storageEngine.newReader();
              StorageNodeCursor nodeCursor = reader.allocateNodeCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isFalse();
        }
    }

    @Test
    void modifyingNodeShouldNotAccidentallyCreateNewBigValueRecordSparse() throws Exception
    {
        modifyingNodeShouldNotAccidentallyCreateNewBigValueRecord( 0 );
    }

    @Test
    void modifyingNodeShouldNotAccidentallyCreateNewBigValueRecordDense() throws Exception
    {
        modifyingNodeShouldNotAccidentallyCreateNewBigValueRecord( 100 );
    }

    private void modifyingNodeShouldNotAccidentallyCreateNewBigValueRecord( int numberOfRelationships ) throws Exception
    {
        // given a node with a property that has a big value
        long nodeId = commandCreationContext.reserveNode();
        long initializeBigValuePosition = storageEngine.stores().bigPropertyValueStore.position();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( 0, stringValue( random.nextAlphaNumericString( 100, 100 ) ) ) ),
                    emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < numberOfRelationships; i++ )
            {
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, nodeId, emptyList() );
            }
        } );
        long bigValuePosition = storageEngine.stores().bigPropertyValueStore.position();
        assertThat( bigValuePosition ).isGreaterThan( initializeBigValuePosition );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( 1, intValue( 10 ) ) ), emptyList(), IntSets.immutable.empty() );
        } );

        // then
        assertThat( storageEngine.stores().bigPropertyValueStore.position() ).isEqualTo( bigValuePosition );
    }

    @Test
    void modifyingNodeShouldNotAccidentallyCreateNewRelationshipBigValueRecordSparse() throws Exception
    {
        modifyingNodeShouldNotAccidentallyCreateNewRelationshipBigValueRecord( 0 );
    }

    @Test
    void modifyingNodeShouldNotAccidentallyCreateNewRelationshipBigValueRecordDense() throws Exception
    {
        modifyingNodeShouldNotAccidentallyCreateNewRelationshipBigValueRecord( 100 );
    }

    private void modifyingNodeShouldNotAccidentallyCreateNewRelationshipBigValueRecord( int numberOfAdditionalRelationships ) throws Exception
    {
        // given a node with a property that has a big value
        long nodeId = commandCreationContext.reserveNode();
        long otherNodeId = commandCreationContext.reserveNode();
        long initializeBigValuePosition = storageEngine.stores().bigPropertyValueStore.position();
        String value = random.nextAlphaNumericString( 100, 100 );
        RelationshipSpec relationship =
                new RelationshipSpec( nodeId, 0, otherNodeId, asSet( new PropertyKeyValue( 0, stringValue( value ) ) ), commandCreationContext );
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitCreatedNode( otherNodeId );
            relationship.create( target );
            for ( int i = 0; i < numberOfAdditionalRelationships; i++ )
            {
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, nodeId, emptyList() );
            }
        } );
        long bigValuePosition = storageEngine.stores().bigPropertyValueStore.position();
        assertThat( bigValuePosition ).isGreaterThan( initializeBigValuePosition );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitNodePropertyChanges( nodeId, singletonList( new PropertyKeyValue( 1, intValue( 10 ) ) ), emptyList(), IntSets.immutable.empty() );
            target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, nodeId, emptyList() );
        } );

        // then
        assertThat( storageEngine.stores().bigPropertyValueStore.position() ).isEqualTo( bigValuePosition );
    }

    @Test
    void shouldCreateUniquenessConstraint() throws Exception
    {
        // given
        long indexId = commandCreationContext.reserveSchema();
        createAndApplyTransaction( target ->
        {
            target.visitAddedIndex( IndexPrototype.uniqueForSchema( SCHEMA_DESCRIPTOR ).withName( "My index" ).materialise( indexId ) );
        } );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitAddedConstraint( ConstraintDescriptorFactory.uniqueForLabel( SCHEMA_DESCRIPTOR.getLabelId(), SCHEMA_DESCRIPTOR.getPropertyIds() )
                    .withName( "Kid A" ).withOwnedIndexId( indexId ) );
        } );

        // then
        List<SchemaRule> schemaRules = storageEngine.stores().schemaStore.loadRules( NULL );
        SchemaRule constraintRule = schemaRules.stream().filter( rule -> rule.getName().equals( "Kid A" ) ).findFirst().get();
        assertThat( constraintRule ).isInstanceOf( ConstraintDescriptor.class );
        ConstraintDescriptor constraintDescriptor = (ConstraintDescriptor) constraintRule;
        assertThat( constraintDescriptor.type() ).isEqualTo( ConstraintType.UNIQUE );
    }

    @Test
    void shouldCreateNodeKeyConstraint() throws Exception
    {
        // given
        long indexId = commandCreationContext.reserveSchema();
        createAndApplyTransaction( target ->
        {
            target.visitAddedIndex( IndexPrototype.uniqueForSchema( SCHEMA_DESCRIPTOR ).withName( "My index" ).materialise( indexId ) );
        } );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitAddedConstraint( ConstraintDescriptorFactory.nodeKeyForLabel( SCHEMA_DESCRIPTOR.getLabelId(), SCHEMA_DESCRIPTOR.getPropertyIds() )
                    .withName( "Kid A" ).withOwnedIndexId( indexId ) );
        } );

        // then
        List<SchemaRule> schemaRules = storageEngine.stores().schemaStore.loadRules( NULL );
        SchemaRule constraintRule = schemaRules.stream().filter( rule -> rule.getName().equals( "Kid A" ) ).findFirst().get();
        assertThat( constraintRule ).isInstanceOf( ConstraintDescriptor.class );
        ConstraintDescriptor constraintDescriptor = (ConstraintDescriptor) constraintRule;
        assertThat( constraintDescriptor.type() ).isEqualTo( ConstraintType.UNIQUE_EXISTS );
    }

    @Test
    void shouldCreateNodePropertyExistenceConstraint() throws Exception
    {
        // given
        createAndApplyTransaction( target ->
        {
            target.visitAddedConstraint( ConstraintDescriptorFactory.existsForLabel( SCHEMA_DESCRIPTOR.getLabelId(), SCHEMA_DESCRIPTOR.getPropertyIds() )
                    .withName( "Kid A" ) );
        } );

        // then
        List<SchemaRule> schemaRules = storageEngine.stores().schemaStore.loadRules( NULL );
        SchemaRule constraintRule = schemaRules.stream().filter( rule -> rule.getName().equals( "Kid A" ) ).findFirst().get();
        assertThat( constraintRule ).isInstanceOf( ConstraintDescriptor.class );
        ConstraintDescriptor constraintDescriptor = (ConstraintDescriptor) constraintRule;
        assertThat( constraintDescriptor.type() ).isEqualTo( ConstraintType.EXISTS );
    }

    @Test
    void shouldCreateAndDeleteRandomDataInMultipleTransactions() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
        } );
        List<Long> nodeIds = new ArrayList<>();
        nodeIds.add( nodeId );
        for ( int i = 0; i < 100; i++ )
        {
            nodeIds.add( commandCreationContext.reserveNode() );
        }
        MutableLongObjectMap<Set<StorageProperty>> expectedOtherNodesProperties = LongObjectMaps.mutable.empty();
        MutableLongSet existenceOfNodeIds = LongSets.mutable.empty();
        existenceOfNodeIds.add( nodeId );

        // when
        MutableLongSet expectedLabels = LongSets.mutable.empty();
        Map<Integer,StorageProperty> expectedNodeProperties = new HashMap<>();
        Set<RelationshipSpec> expectedRelationships = new HashSet<>();
        int numRounds = 30;
        for ( int r = 0; r < numRounds; r++ )
        {
            int round = r;
            createAndApplyTransaction( target ->
            {
                // === random label changes
                int numLabelChanges = random.nextInt( 4 );
                MutableLongSet addedLabels = LongSets.mutable.empty();
                MutableLongSet removedLabels = LongSets.mutable.empty();
                for ( int i = 0; i < numLabelChanges; i++ )
                {
                    long labelId = random.nextInt( 10 );
                    (expectedLabels.contains( labelId ) ? removedLabels : addedLabels).add( labelId );
                }
                target.visitNodeLabelChanges( nodeId, addedLabels, removedLabels );
                expectedLabels.addAll( addedLabels );
                expectedLabels.removeAll( removedLabels );

                // === random property changes
                int numPropertyChanges = random.nextInt( 4 );
                Set<StorageProperty> addedProperties = new HashSet<>();
                Set<StorageProperty> changedProperties = new HashSet<>();
                MutableIntSet removedProperties = IntSets.mutable.empty();
                for ( int i = 0; i < numPropertyChanges; i++ )
                {
                    int propertyKeyId = random.nextInt( 10 );
                    if ( expectedNodeProperties.containsKey( propertyKeyId ) )
                    {
                        if ( random.nextBoolean() )
                        {
                            removedProperties.add( propertyKeyId );
                        }
                        else
                        {
                            changedProperties.add( new PropertyKeyValue( propertyKeyId, random.nextValue() ) );
                        }
                    }
                    else
                    {
                        addedProperties.add( new PropertyKeyValue( propertyKeyId, random.nextValue() ) );
                    }
                }
                target.visitNodePropertyChanges( nodeId, addedProperties, changedProperties, removedProperties );
                addedProperties.forEach( p -> expectedNodeProperties.put( p.propertyKeyId(), p ) );
                changedProperties.forEach( p -> expectedNodeProperties.put( p.propertyKeyId(), p ) );
                removedProperties.forEach( expectedNodeProperties::remove );

                // === random relationship changes
                Set<RelationshipSpec> createdRelationships = new HashSet<>();
                int relationshipCountCap = round < 10 ? 60 : Integer.MAX_VALUE;
                for ( int i = 0; i < numRounds - round && (expectedRelationships.size() + createdRelationships.size()) < relationshipCountCap; i++ )
                {
                    long otherNodeId = random.among( nodeIds );
                    if ( existenceOfNodeIds.add( otherNodeId ) )
                    {
                        target.visitCreatedNode( otherNodeId );
                        Set<StorageProperty> otherNodeProperties = randomProperties( random.nextInt( 3 ) );
                        target.visitNodePropertyChanges( otherNodeId, otherNodeProperties, emptyList(), IntSets.immutable.empty() );
                        expectedOtherNodesProperties.put( otherNodeId, otherNodeProperties );
                    }
                    long startNode;
                    long endNode;
                    if ( random.nextBoolean() )
                    {
                        startNode = nodeId;
                        endNode = otherNodeId;
                    }
                    else
                    {
                        startNode = otherNodeId;
                        endNode = nodeId;
                    }
                    RelationshipSpec relationship =
                            createRelationship( target, commandCreationContext, startNode, random.nextInt( 4 ), endNode, randomProperties(
                                    random.nextInt( 3 ) ) );
                    createdRelationships.add( relationship );
                }
                for ( int i = 0; i < round && !expectedRelationships.isEmpty(); i++ )
                {
                    RelationshipSpec relationship = random.among( new ArrayList<>( expectedRelationships ) );
                    target.visitDeletedRelationship( relationship.id, relationship.type, relationship.startNodeId, relationship.endNodeId );
                    expectedRelationships.remove( relationship );
                }
                expectedRelationships.addAll( createdRelationships );
            } );

            // then
            try ( StorageReader storageReader = storageEngine.newReader();
                    StorageNodeCursor nodeCursor = storageReader.allocateNodeCursor( NULL );
                    StoragePropertyCursor propertyCursor = storageReader.allocatePropertyCursor( NULL, EmptyMemoryTracker.INSTANCE );
                    StorageRelationshipTraversalCursor relationshipCursor = storageReader.allocateRelationshipTraversalCursor( NULL ) )
            {
                List<Runnable> checks = new ArrayList<>();
                checks.add( () -> assertContentsOfNode( nodeId, expectedLabels, new HashSet<>( expectedNodeProperties.values() ),
                        expectedRelationships, nodeCursor, propertyCursor, relationshipCursor ) );
                checks.add( () -> expectedOtherNodesProperties.forEachKeyValue( ( otherNodeId, otherNodeProperties ) ->
                {
                    Set<RelationshipSpec> otherNodeRelationships =
                            expectedRelationships.stream().filter( rel -> rel.startNodeId == otherNodeId || rel.endNodeId == otherNodeId ).collect(
                                    Collectors.toSet() );
                    assertContentsOfNode( otherNodeId, LongSets.immutable.empty(), otherNodeProperties, otherNodeRelationships,
                            nodeCursor, propertyCursor, relationshipCursor );
                } ) );
                if ( random.nextBoolean() )
                {
                    Collections.reverse( checks );
                }
                checks.forEach( Runnable::run );
            }
        }
    }

    @Test
    void shouldConcurrentlyCreateRelationshipsOnDenseNodes() throws Exception
    {
        long[] nodeIds = new long[4];
        for ( int i = 0; i < nodeIds.length; i++ )
        {
            nodeIds[i] = commandCreationContext.reserveNode();
        }
        // first make them dense
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            for ( long nodeId : nodeIds )
            {
                target.visitCreatedNode( nodeId );
            }
            for ( int i = 0; i < 100 * nodeIds.length; i++ )
            {
                long startNode = nodeIds[random.nextInt( nodeIds.length )];
                long endNode = nodeIds[random.nextInt( nodeIds.length )];
                RelationshipSpec relationship = new RelationshipSpec( startNode, 0, endNode, emptySet(), commandCreationContext );
                relationship.create( target );
                relationships.add( relationship );
            }
        } );

        // when letting a couple of threads try to create or delete relationships for them
        ConcurrentLinkedQueue<RelationshipSpec> createdRelationships = new ConcurrentLinkedQueue<>();
        Race race = new Race().withEndCondition( () -> false );
        race.addContestants( nodeIds.length, id -> throwing( () ->
        {
            long nodeId = nodeIds[id];
            createAndApplyTransaction( target ->
            {
                long otherNodeId = commandCreationContext.reserveNode();
                target.visitCreatedNode( otherNodeId );
                RelationshipSpec relationship = new RelationshipSpec( nodeId, 1, otherNodeId, emptySet(), commandCreationContext );
                relationship.create( target );
                createdRelationships.add( relationship );
            } );
        } ), 1_000 );
        race.goUnchecked();

        // then
        relationships.addAll( createdRelationships );
        for ( long nodeId : nodeIds )
        {
            assertContentsOfNode( nodeId, LongSets.immutable.empty(), emptySet(),
                    relationships.stream().filter( r -> r.startNodeId == nodeId || r.endNodeId == nodeId ).collect( Collectors.toSet() ) );
        }
    }

    @Test
    void shouldSelectRelationshipsToSpecificNeighbourNode() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        createAndApplyTransaction( target -> target.visitCreatedNode( nodeId ) );
        int numTypes = 4;

        // when
        Set<RelationshipSpec> relationships = new HashSet<>();
        List<Long> otherNodeIds = new ArrayList<>();
        for ( int r = 0; r < 50; r++ )
        {
            createAndApplyTransaction( target ->
            {
                for ( int i = 0; i < 10; i++ )
                {
                    int type = random.nextInt( numTypes );
                    long otherNodeId;
                    if ( otherNodeIds.isEmpty() || random.nextFloat() < 0.2 )
                    {
                        // Create a new other node
                        otherNodeId = this.commandCreationContext.reserveNode();
                        target.visitCreatedNode( otherNodeId );
                        otherNodeIds.add( otherNodeId );
                    }
                    else
                    {
                        // Reuse an existing other node
                        otherNodeId = random.among( otherNodeIds );
                    }
                    boolean outgoing = random.nextBoolean();
                    long startNode = outgoing ? nodeId : otherNodeId;
                    long endNode = outgoing ? otherNodeId : nodeId;
                    RelationshipSpec relationship = new RelationshipSpec( startNode, type, endNode, emptySet(), commandCreationContext );
                    relationships.add( relationship );
                    relationship.create( target );
                }
            } );

            // then assert relationshipsTo here for all neighbour nodes
            for ( Long otherNodeId : otherNodeIds )
            {
                for ( int type = 0; type < numTypes; type++ )
                {
                    assertRelationshipsTo( nodeId, relationships, selection( type, Direction.BOTH ), otherNodeId );
                    assertRelationshipsTo( nodeId, relationships, selection( type, Direction.OUTGOING ), otherNodeId );
                    assertRelationshipsTo( nodeId, relationships, selection( type, Direction.INCOMING ), otherNodeId );
                }
            }
        }
    }

    @Test
    void shouldReusePropertyCursorBetweenDenseRelationshipsAndNodes() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        Set<StorageProperty> relationshipProperties = asSet( new PropertyKeyValue( 1, stringValue( "abc" ) ), new PropertyKeyValue( 2, longValue( 123 ) ) );
        Set<StorageProperty> nodeProperties = asSet( new PropertyKeyValue( 1, stringValue( "rts" ) ) );
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            for ( int i = 0; i < 200; i++ )
            {
                long otherNodeId = commandCreationContext.reserveNode();
                target.visitCreatedNode( otherNodeId );
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, otherNodeId,
                        relationshipProperties );
            }
            target.visitNodePropertyChanges( nodeId, nodeProperties, emptyList(), IntSets.immutable.empty() );
        } );

        // when
        try ( var reader = storageEngine.newReader();
                var nodeCursor = reader.allocateNodeCursor( NULL );
                var relationshipCursor = reader.allocateRelationshipTraversalCursor( NULL );
                var propertyCursor = reader.allocatePropertyCursor( NULL, EmptyMemoryTracker.INSTANCE ) )
        {
            nodeCursor.single( nodeId );
            nodeCursor.next();
            nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
            assertThat( relationshipCursor.next() ).isTrue();

            // then
            relationshipCursor.properties( propertyCursor );
            assertProperties( relationshipProperties, propertyCursor );
            nodeCursor.properties( propertyCursor );
            assertProperties( nodeProperties, propertyCursor );
        }
    }

    @Test
    void shouldChangeRelationshipProperties() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        createAndApplyTransaction( target -> target.visitCreatedNode( nodeId ) );

        // when
        Set<RelationshipSpec> expectedRelationships = new HashSet<>();
        for ( int t = 0; t < 30; t++ )
        {
            createAndApplyTransaction( target ->
            {
                // Change some properties on existing relationships
                RelationshipSpec[] existingRelationships = expectedRelationships.toArray( new RelationshipSpec[0] );
                int numRelationshipsChanged = min( existingRelationships.length, random.nextInt( 1, 10 ) );
                MutableLongSet changedRelationships = LongSets.mutable.empty();
                for ( int r = 0; r < numRelationshipsChanged; r++ )
                {
                    RelationshipSpec relationship = random.among( existingRelationships );
                    if ( !changedRelationships.add( relationship.id ) )
                    {
                        r--;
                        continue;
                    }

                    List<StorageProperty> added = new ArrayList<>();
                    List<StorageProperty> changed = new ArrayList<>();
                    MutableIntSet removed = IntSets.mutable.empty();
                    int numChanges = min( relationship.properties.size(), random.nextInt( 1, 3 ) );
                    Map<Integer,Value> map = relationship.properties.stream().collect( toMap( StorageProperty::propertyKeyId, StorageProperty::value ) );
                    MutableIntSet changedKeys = IntSets.mutable.empty();
                    for ( int c = 0; c < numChanges; c++ )
                    {
                        int key = random.nextInt( 10 );
                        if ( !changedKeys.add( key ) )
                        {
                            c--;
                            continue;
                        }
                        Value existingValue = map.get( key );
                        if ( existingValue != null )
                        {
                            if ( random.nextBoolean() )
                            {
                                Value newValue = random.nextValue();
                                changed.add( new PropertyKeyValue( key, newValue ) );
                                map.put( key, newValue );
                            }
                            else
                            {
                                removed.add( key );
                                map.remove( key );
                            }
                        }
                        else
                        {
                            Value newValue = random.nextValue();
                            added.add( new PropertyKeyValue( key, newValue ) );
                            map.put( key, newValue );
                        }
                    }

                    target.visitRelPropertyChanges( relationship.id, relationship.type, relationship.startNodeId, relationship.endNodeId,
                            added, changed, removed );
                    relationship.properties.clear();
                    map.forEach( ( key, value ) -> relationship.properties.add( new PropertyKeyValue( key, value ) ) );
                }

                // Create some more relationships
                for ( int i = 0; i < 10; i++ )
                {
                    RelationshipSpec relationship =
                            new RelationshipSpec( nodeId, 0, nodeId, randomProperties( random.nextInt( 1, 5 ) ), commandCreationContext );
                    expectedRelationships.add( relationship );
                    relationship.create( target );
                }
            } );

            assertContentsOfNode( nodeId, LongSets.immutable.empty(), emptySet(), expectedRelationships );
        }
    }

    @Test
    void shouldFormXLChainBeforeDense() throws Exception
    {
        // given
        long nodeId = commandCreationContext.reserveNode();
        MutableLongList otherNodes = LongLists.mutable.empty();
        for ( int i = 0; i < 100; i++ )
        {
            otherNodes.add( commandCreationContext.reserveNode() );
        }
        int x8Size = Record.recordSize( Record.sizeExpFromXFactor( 8 ) );
        Set<StorageProperty> properties = new HashSet<>();
        Value prop = Values.byteArray( new byte[]{0, 1, 2, 3, 4, 5, 6} ); //this will generate 10B data (header + length + data + key)
        int sizePerProp = 10;
        int propSize = (int) (x8Size * 0.8);
        for ( int i = 0; i < propSize / sizePerProp; i++ )
        {
            properties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + i + 1, prop ) );
        }

        int relsToAdd = 100;
        Set<RelationshipSpec> relationships = new HashSet<>();

        createAndApplyTransaction( target ->
        {
            otherNodes.forEach( target::visitCreatedNode );
        } );
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodePropertyChanges( nodeId, properties, emptyList(), IntSets.immutable.empty() );
            for ( int i = 0; i < relsToAdd; i++ )
            {
                RelationshipSpec relationship =
                        new RelationshipSpec( nodeId, i % 3, otherNodes.get( i % otherNodes.size() ), emptySet(), commandCreationContext );
                relationship.create( target );
                relationships.add( relationship );
            }
        } );

        assertContentsOfNode( nodeId, LongSets.immutable.empty(), properties, relationships );
        assertXLChainLength( nodeId, 2, false, true );
    }

    @TestFactory
    Collection<DynamicTest> shouldHandlePermutationsOfDataBlocksXLChains()
    {
        List<DynamicTest> tests = new ArrayList<>();
        Set<Double> fillRates = Set.of( 0d, 0.8d, 1.5d, 3.5d );
        for ( double labelsFillRate : fillRates )
        {
            for ( double propertiesFillRate : fillRates )
            {
                for ( double degreesFillRate : fillRates )
                {
                    double totalSize = labelsFillRate + propertiesFillRate + degreesFillRate;
                    if ( totalSize > .1 ) //skip case with no fill
                    {
                        tests.add( createXLChainTest( labelsFillRate, propertiesFillRate, Double.min( 0.8D, degreesFillRate ) ) );
                    }
                }
            }
        }
        return tests;
    }

    private DynamicTest createXLChainTest( double labelXLFill, double propertiesXLFill, double degreesXLFill )
    {
        Executable test = new Executable()
        {
            @Override
            public void execute() throws Throwable
            {
                int x8Size = Record.recordSize( Record.sizeExpFromXFactor( 8 ) );

                MutableLongSet labels = LongSets.mutable.empty();
                double labelsSize = x8Size * labelXLFill;
                double sizePerLabel = 1.3;
                for ( int i = 0; i < labelsSize / sizePerLabel; i++ )
                {
                    labels.add( SCHEMA_DESCRIPTOR.getLabelId() + i + 1 );
                }
                labels.add( SCHEMA_DESCRIPTOR.getLabelId() );

                Set<StorageProperty> properties = new HashSet<>();
                Value prop = Values.byteArray( new byte[]{0, 1, 2, 3, 4, 5, 6} ); //this will generate 10B data (header + length + data + key)
                int sizePerProp = 10;
                int propSize = (int) (x8Size * propertiesXLFill);
                for ( int i = 0; i < propSize / sizePerProp; i++ )
                {
                    properties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + i + 1, prop ) );
                }
                PropertyKeyValue before = new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), Values.intValue( 25 ) );
                properties.add( before );

                Set<RelationshipSpec> relationships = new HashSet<>();
                double degSize = x8Size * degreesXLFill;
                int numRelsOfDifferentType = (int) (degSize / 6);

                MutableLong nodeId = new MutableLong();
                shouldGenerateIndexUpdates( target -> { /* do nothing */ }, target ->
                {
                    CommandCreationContext txContext = storageEngine.newCommandCreationContext( NULL, EmptyMemoryTracker.INSTANCE );
                    long node = txContext.reserveNode();
                    nodeId.setValue( node );
                    target.visitCreatedNode( node );
                    target.visitNodeLabelChanges( node, labels, LongSets.immutable.empty() );
                    target.visitNodePropertyChanges( node, properties, Collections.emptyList(), IntSets.immutable.empty() );
                    if ( numRelsOfDifferentType > 0 )
                    {
                        long otherNode = txContext.reserveNode();
                        target.visitCreatedNode( otherNode );
                        int relPad = x8Size / 2;
                        int relType = 0;
                        for ( int i = 0; i < numRelsOfDifferentType + relPad; i++ )
                        {
                            //only one rel/type ensures big degrees block
                            RelationshipSpec relationshipSpec = new RelationshipSpec( node, relType, otherNode, emptySet(), txContext );
                            relationships.add( relationshipSpec );
                            relationshipSpec.create( target );

                            if ( i < numRelsOfDifferentType )
                            {
                                relType++;
                            }
                        }
                    }
                }, index -> asSet( IndexEntryUpdate.add( nodeId.longValue(), index, before.value()) ) );
                assertContentsOfNode( nodeId.longValue(), labels, properties, relationships );
                assertXLChainLength( nodeId.longValue(), (int) (labelXLFill + propertiesXLFill + degreesXLFill + 0.65), degreesXLFill > 1e-3, false );
            }

            @Override
            public String toString()
            {
                return String.format( "XL chains - Labels:%.1f Properties:%.1f Degrees:%.1f", labelXLFill, propertiesXLFill, degreesXLFill );
            }
        };
        return DynamicTest.dynamicTest( test.toString(), test );
    }

    @TestFactory
    Collection<DynamicTest> shouldSeeAllLabelPropertiesPermutations()
    {
        List<DynamicTest> tests = new ArrayList<>();
        /*
            -       ->  x1
            -       ->  x1 + XL
            x1      ->  -
            x1 + XL ->  -
            x1      ->  x1 + XL
            x1 + XL ->  x1
            x1 + x4 ->  x1 + x8
            x1 + x8 ->  x1 + x4
         */
        tests.add( createIndexPermutationTest( 0, 1, false, false ) );
        tests.add( createIndexPermutationTest( 1, 0, false, false ) );
        for ( boolean labelsInXL : Set.of( true, false ) )
        {
            for ( boolean propsInXL : Set.of( true, false ) )
            {
                //avoid using x2 to make the code cleaner when ensuring both labels and props in XL
                tests.add( createIndexPermutationTest( 0, 4, labelsInXL, propsInXL ) );
                tests.add( createIndexPermutationTest( 4, 0, labelsInXL, propsInXL ) );
                tests.add( createIndexPermutationTest( 1, 4, labelsInXL, propsInXL ) );
                tests.add( createIndexPermutationTest( 4, 1, labelsInXL, propsInXL ) );
                tests.add( createIndexPermutationTest( 4, 8, labelsInXL, propsInXL ) );
                tests.add( createIndexPermutationTest( 8, 4, labelsInXL, propsInXL ) );
            }
        }

        return tests;
    }

    private DynamicTest createIndexPermutationTest( int beforeX, int afterX, boolean labelsInXL, boolean propsInXL )
    {
        Executable test = new Executable()
        {
            @Override
            public void execute() throws Throwable
            {
                int beforeSize = beforeX == 0 ? 0 : Record.recordSize( Record.sizeExpFromXFactor( beforeX ) );
                int afterSize = afterX == 0 ? 0 : Record.recordSize( Record.sizeExpFromXFactor( afterX ) );
                assertNotEquals( beforeSize, afterSize );

                MutableLongSet labels = LongSets.mutable.empty();
                Set<StorageProperty> properties = new HashSet<>();

                IntValue value = Values.intValue( 80 );
                IntValue changedValue = Values.intValue( 90 );

                int labelsSize = 2;
                int propsSize = 3;
                if ( labelsInXL )
                {
                    for ( int i = 0; i < Record.SIZE_BASE; i++ )
                    {
                        labels.add( SCHEMA_DESCRIPTOR.getLabelId() + i + 1 );
                        labelsSize += 1;
                    }
                }
                if ( propsInXL )
                {
                    Value prop = Values.byteArray( new byte[]{0, 1, 2, 3, 4, 5, 6} ); //this will generate 10B data (header + length + data + key)
                    int sizePerProp = 10;
                    for ( int i = 0; i < Record.SIZE_BASE / sizePerProp; i++ )
                    {
                        properties.add( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId() + i + 1, prop ) );
                        propsSize += sizePerProp;
                    }
                }
                int labelsSizeBefore = beforeX > 1 ? labelsSize : 0;
                int propsSizeBefore = beforeX > 1 ? propsSize : 0;
                int labelsSizeAfter = afterX > 1 ? labelsSize : 0;
                int propsSizeAfter = afterX > 1 ? propsSize : 0;

                long nodeId = commandCreationContext.reserveNode();
                long otherNodeId = commandCreationContext.reserveNode();

                int beforeFillSize = beforeSize * 4 / 5; //should ensure correct record
                int afterFillSize = afterSize * 4 / 5; //should ensure correct record

                int relSize = 4;
                int relsToAddBefore = beforeX > 1 ? (beforeFillSize - labelsSizeBefore - propsSizeBefore) / relSize : 0;
                int relsToRemoveAfter = afterX == 1
                                        ? relsToAddBefore
                                        : ( afterX < beforeX
                                            ? relsToAddBefore - ((afterFillSize - labelsSizeAfter - propsSizeAfter) / relSize)
                                            : 0);
                int relsToAddAfter = afterX != 1 && afterX > beforeX
                                     ? ((afterFillSize - labelsSizeAfter - propsSizeAfter) / relSize) - relsToAddBefore
                                     : 0;

                shouldGenerateIndexUpdates( target ->
                {
                    //Setup (before state)
                    target.visitCreatedNode( otherNodeId );
                    if ( beforeX != 0 )
                    {
                        target.visitCreatedNode( nodeId );
                        target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
                        target.visitNodePropertyChanges( nodeId, List.of( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                                IntSets.immutable.empty() );

                        //padding to get correct before record size
                        if ( beforeX > 1 )
                        {
                            //padding
                            for ( int i = 0; i < relsToAddBefore; i++ )
                            {
                                target.visitCreatedRelationship( i, 0, nodeId, otherNodeId, emptyList());
                            }
                            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
                            target.visitNodePropertyChanges( nodeId, properties, emptyList(), IntSets.immutable.empty() );
                        }
                    }

                }, target ->
                {
                    assertCorrectSizeXWithLabelPropertyLocation( nodeId, beforeX, labelsInXL, propsInXL );
                    //Changes (after state)
                    if ( afterX != 0 )
                    {
                        if ( beforeX == 0 )
                        {
                            target.visitCreatedNode( nodeId );
                            target.visitNodeLabelChanges( nodeId, LongSets.immutable.of( SCHEMA_DESCRIPTOR.getLabelId() ), LongSets.immutable.empty() );
                            target.visitNodePropertyChanges( nodeId, List.of( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), value ) ), emptyList(),
                                    IntSets.immutable.empty() );
                        }
                        else
                        {
                            if ( beforeX > 1 && afterX <= 1 )
                            {
                                target.visitNodeLabelChanges( nodeId,  LongSets.immutable.empty(), labels );
                                var toRemove =
                                        IntSets.immutable.ofAll( properties.stream().map( StorageProperty::propertyKeyId ).collect( Collectors.toList() ) );
                                target.visitNodePropertyChanges( nodeId, emptyList(), emptyList(), toRemove );
                            }
                            target.visitNodePropertyChanges( nodeId, emptyList(),
                                    List.of( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), changedValue ) ), IntSets.immutable.empty() );
                        }

                        //padding to get labels/props in correct record
                        if ( beforeX <= 1 && afterX > 1 )
                        {
                            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
                            target.visitNodePropertyChanges( nodeId, properties, emptyList(), IntSets.immutable.empty() );
                        }

                        if ( afterX > 1 )
                        {
                            for ( int i = 0; i < relsToAddAfter; i++ )
                            {
                                target.visitCreatedRelationship( i + relsToAddBefore, 0, nodeId, otherNodeId, emptyList());
                            }
                        }
                        if ( beforeX > 1 )
                        {
                            for ( int i = 0; i < relsToRemoveAfter; i++ )
                            {
                                target.visitDeletedRelationship( i, 0, nodeId, otherNodeId );
                            }
                        }
                    }
                    else
                    {
                        for ( int i = 0; i < relsToAddBefore; i++ )
                        {
                            target.visitDeletedRelationship( i, 0, nodeId, otherNodeId );
                        }
                        target.visitDeletedNode( nodeId );
                    }
                }, index -> {
                    assertCorrectSizeXWithLabelPropertyLocation( nodeId, afterX, labelsInXL, propsInXL );
                    //Expected index updates
                    Set<IndexEntryUpdate<IndexDescriptor>> updates = new HashSet<>();
                    if ( afterX == 0 )
                    {
                        updates.add( IndexEntryUpdate.remove( nodeId, index, value ) );
                    }
                    else
                    {
                        if ( beforeX == 0 )
                        {
                            updates.add( IndexEntryUpdate.add( nodeId, index, value ) );
                        }
                        else
                        {
                            updates.add( IndexEntryUpdate.change( nodeId, index, value, changedValue ) );
                        }
                    }
                    return updates;
                }  );
            }

            @Override
            public String toString()
            {
                return String.format( "IndexUpdatesTest:%s %s %s to %s",
                        propsInXL ? " PropertiesInXL " : " ",
                        labelsInXL ? " LabelsInXL " : " ",
                        beforeX == 0 ? "created" : "x" + beforeX,
                        afterX == 0 ? "deleted" : "x" + afterX
                );
            }
        };
        return DynamicTest.dynamicTest( test.toString(), test );
    }

    private void assertCorrectSizeXWithLabelPropertyLocation( long nodeId, int sizeX, boolean labelsInXL, boolean propsInXL )
    {
        try ( var storageReader = storageEngine.newReader();
              var nodeCursor = storageReader.allocateNodeCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isEqualTo( sizeX > 0 );
            if ( sizeX > 1 )
            {
                assertThat( nodeCursor.data.xLChainStartPointer ).isNotEqualTo( FrekiMainStoreCursor.NULL );
                assertThat( nodeCursor.data.header.hasReferenceMark( Header.FLAG_LABELS ) ).isEqualTo( labelsInXL );
                assertThat( nodeCursor.data.header.hasReferenceMark( Header.OFFSET_PROPERTIES ) ).isEqualTo( propsInXL );
                int sizeExp = sizeExponentialFromRecordPointer( nodeCursor.data.xLChainStartPointer );
                assertThat( sizeX ).isEqualTo( Record.recordXFactor( sizeExp ) );
            }
            else if ( sizeX == 1 )
            {
                assertThat( nodeCursor.data.xLChainStartPointer ).isEqualTo( FrekiMainStoreCursor.NULL );
            }
        }
    }

    private void assertXLChainLength( long nodeId, int expectedLength, boolean expectedDense, boolean exactLengthMatch )
    {
        try ( var storageReader = storageEngine.newReader();
                var nodeCursor = storageReader.allocateNodeCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isTrue();
            assertThat( nodeCursor.data.isDense ).isEqualTo( expectedDense );
            assertThat( nodeCursor.data.xLChainStartPointer ).isEqualTo( nodeCursor.data.xLChainNextLinkPointer ); //we have not yet loaded the chain

            int actualChainLength = 0;
            while ( nodeCursor.loadNextChainLink() )
            {
                actualChainLength++;
            }
            if ( exactLengthMatch )
            {
                assertThat( actualChainLength ).isEqualTo( expectedLength );
            }
            else
            {
                assertThat( actualChainLength ).isGreaterThanOrEqualTo( expectedLength );
            }
        }
    }

    private void assertRelationshipsTo( long nodeId, Set<RelationshipSpec> relationships, RelationshipSelection selection, long otherNodeId )
    {
        try ( var storageReader = storageEngine.newReader();
                var nodeCursor = storageReader.allocateNodeCursor( NULL );
                var propertyCursor = storageReader.allocatePropertyCursor( NULL, EmptyMemoryTracker.INSTANCE );
                var relationshipsCursor = storageReader.allocateRelationshipTraversalCursor( NULL ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isTrue();
            nodeCursor.relationshipsTo( relationshipsCursor, selection, otherNodeId );
            Set<RelationshipSpec> readRelationships = readRelationships( propertyCursor, relationshipsCursor );
            Set<RelationshipSpec> expectedRelationships =
                    relationships.stream().filter( r -> selection.test( r.type, r.direction( nodeId ) ) && r.neighbourNode( nodeId ) == otherNodeId ).collect(
                            Collectors.toSet() );
            assertThat( readRelationships ).isEqualTo( expectedRelationships );
        }
    }

    private Set<StorageProperty> randomProperties( int numProperties )
    {
        Set<StorageProperty> properties = new HashSet<>();
        for ( int i = 0; i < numProperties; i++ )
        {
            properties.add( new PropertyKeyValue( i, random.nextValue() ) );
        }
        return properties;
    }

    // TODO: 2020-03-06 addingBigValueRelationshipPropertyShouldOnlyCreateOneShared (sparse/dense)

    // TODO since we don't quite support removing and reusing big value record space then wait with these until we do
    // TODO: 2020-03-06 removingBigValueNodePropertyShouldAlsoRemoveItsBigValueRecord
    // TODO: 2020-03-06 changingBigValueNodePropertyShouldAlsoRemoveThePreviousBigValueRecord

    private void shouldGenerateIndexUpdates(
            ThrowingConsumer<TxStateVisitor,Exception> beforeState, ThrowingConsumer<TxStateVisitor,Exception> testState,
            Function<IndexDescriptor,Set<IndexEntryUpdate<IndexDescriptor>>> expectedUpdates ) throws Exception
    {
        // given
        IndexDescriptor index = createIndex( SCHEMA_DESCRIPTOR );
        if ( beforeState != null )
        {
            createAndApplyTransaction( beforeState );
        }
        indexUpdateListener.clear();

        // when
        createAndApplyTransaction( testState );

        // then
        Set<IndexEntryUpdate<IndexDescriptor>> actual = Iterables.asSet( indexUpdateListener.updates );
        Set<IndexEntryUpdate<IndexDescriptor>> expected = expectedUpdates.apply( index );
        assertThat( actual ).isEqualTo( expected );
    }

    private IndexDescriptor createIndex( SchemaDescriptor schemaDescriptor ) throws Exception
    {
        IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "freki", "1" );
        IndexDescriptor indexDescriptor = IndexPrototype.forSchema( schemaDescriptor, providerDescriptor ).withName( "the-index" ).materialise( 8 );
        createAndApplyTransaction( target ->
        {
            target.visitAddedIndex( indexDescriptor );
        } );
        return indexDescriptor;
    }

    private void assertContentsOfNode( long nodeId, LongSet labelIds, Set<StorageProperty> nodeProperties, Set<RelationshipSpec> relationships )
    {
        try ( StorageReader storageReader = storageEngine.newReader();
                StorageNodeCursor nodeCursor = storageReader.allocateNodeCursor( NULL );
                StoragePropertyCursor propertyCursor = storageReader.allocatePropertyCursor( NULL, EmptyMemoryTracker.INSTANCE );
                StorageRelationshipTraversalCursor relationshipCursor = storageReader.allocateRelationshipTraversalCursor( NULL ) )
        {
            assertContentsOfNode( nodeId, labelIds, nodeProperties, relationships, nodeCursor, propertyCursor, relationshipCursor );
        }
    }

    private void assertContentsOfNode( long nodeId, LongSet labelIds, Set<StorageProperty> nodeProperties, Set<RelationshipSpec> relationships,
            StorageNodeCursor nodeCursor, StoragePropertyCursor propertyCursor, StorageRelationshipTraversalCursor relationshipCursor )
    {
        // labels
        nodeCursor.single( nodeId );
        assertThat( nodeCursor.next() ).isTrue();
        assertArrayEquals( labelIds.toSortedArray(), nodeCursor.labels() );
        nodeLabelUpdateListener.assertNodeHasLabels( nodeId, labelIds );

        // properties
        nodeCursor.properties( propertyCursor );
        assertProperties( nodeProperties, propertyCursor );

        // relationships
        nodeCursor.relationships( relationshipCursor, ALL_RELATIONSHIPS );
        Set<RelationshipSpec> readRelationships = readRelationships( propertyCursor, relationshipCursor );
        assertThat( readRelationships ).isEqualTo( relationships );

        // degrees
        EagerDegrees degrees = new EagerDegrees();
        nodeCursor.degrees( ALL_RELATIONSHIPS, degrees, true );
        Degrees expectedDegrees = buildExpectedDegrees( nodeId, relationships );
        assertThat( IntSets.immutable.of( degrees.types() ) ).isEqualTo( IntSets.immutable.of( expectedDegrees.types() ) );
        for ( int type : degrees.types() )
        {
            assertThat( degrees.outgoingDegree( type ) ).isEqualTo( expectedDegrees.outgoingDegree( type ) );
            assertThat( degrees.incomingDegree( type ) ).isEqualTo( expectedDegrees.incomingDegree( type ) );
            assertThat( degrees.totalDegree( type ) ).isEqualTo( expectedDegrees.totalDegree( type ) );
        }
    }

    private Set<RelationshipSpec> readRelationships( StoragePropertyCursor propertyCursor, StorageRelationshipTraversalCursor relationshipCursor )
    {
        Set<RelationshipSpec> readRelationships = new HashSet<>();
        while ( relationshipCursor.next() )
        {
            relationshipCursor.properties( propertyCursor );
            RelationshipSpec relationship =
                    new RelationshipSpec( relationshipCursor.sourceNodeReference(), relationshipCursor.type(), relationshipCursor.targetNodeReference(),
                            readProperties( propertyCursor ), relationshipCursor.entityReference() );
            readRelationships.add( relationship );
        }
        return readRelationships;
    }

    private static Degrees buildExpectedDegrees( long nodeId, Set<RelationshipSpec> relationships )
    {
        EagerDegrees degrees = new EagerDegrees();
        relationships.forEach( relationship -> degrees.add( relationship.type, relationship.direction( nodeId ), 1 ) );
        return degrees;
    }

    private void assertProperties( Set<StorageProperty> expectedProperties, StoragePropertyCursor propertyCursor )
    {
        Set<StorageProperty> readNodeProperties = readProperties( propertyCursor );
        assertThat( readNodeProperties ).isEqualTo( expectedProperties );
    }

    private Set<StorageProperty> readProperties( StoragePropertyCursor propertyCursor )
    {
        Set<StorageProperty> readProperties = new HashSet<>();
        while ( propertyCursor.next() )
        {
            assertThat( readProperties.add( new PropertyKeyValue( propertyCursor.propertyKey(), propertyCursor.propertyValue() ) ) ).isTrue();
        }
        return readProperties;
    }

    private void createAndApplyTransaction( ThrowingConsumer<TxStateVisitor,Exception> data ) throws Exception
    {
        Collection<StorageCommand> commands = createCommands( data );
        applyCommands( commands );
        commandCreationContext.reset();
    }

    private Collection<StorageCommand> createCommands( ThrowingConsumer<TxStateVisitor,Exception> data ) throws KernelException
    {
        Collection<StorageCommand> commands = new ArrayList<>();
        try ( StorageReader reader = storageEngine.newReader() )
        {
            ReadableTransactionState transactionState = mock( ReadableTransactionState.class );
            NodeState emptyNodeState = mock( NodeState.class );
            when( transactionState.getNodeState( anyLong() ) ).thenReturn( emptyNodeState );
            when( emptyNodeState.labelDiffSets() ).thenReturn( LongDiffSets.EMPTY );
            doAnswer( invocationOnMock ->
            {
                TxStateVisitor visitor = invocationOnMock.getArgument( 0, TxStateVisitor.class );
                data.accept( visitor );
                return null;
            } ).when( transactionState ).accept( any() );
            storageEngine.createCommands( commands, transactionState, reader, commandCreationContext,
                    ResourceLocker.IGNORE, TransactionIdStore.BASE_TX_ID, NO_DECORATION, NULL );
        }
        return commands;
    }

    private void applyCommands( Collection<StorageCommand> commands ) throws Exception
    {
        storageEngine.apply( new SingleTxToApply( commands ), TransactionApplicationMode.EXTERNAL );
    }

    private RelationshipSpec createRelationship( TxStateVisitor target, CommandCreationContext commandCreationContext, long startNodeId, int type,
            long otherNodeId, Set<StorageProperty> properties )
    {
        RelationshipSpec relationship = new RelationshipSpec( startNodeId, type, otherNodeId, properties, commandCreationContext );
        relationship.create( target );
        return relationship;
    }

    private static class SimpleTokenCreator implements TokenCreator
    {
        private final AtomicInteger highId = new AtomicInteger( 1 );

        @Override
        public int createToken( String name, boolean internal )
        {
            return highId.incrementAndGet();
        }
    }

    private static class NoopIndexConfigCompletor implements IndexConfigCompleter
    {
        @Override
        public IndexDescriptor completeConfiguration( IndexDescriptor index )
        {
            return index;
        }
    }

    private static class SingleTxToApply implements CommandsToApply
    {
        private final Collection<StorageCommand> commands;

        SingleTxToApply( Collection<StorageCommand> commands )
        {
            this.commands = commands;
        }

        @Override
        public long transactionId()
        {
            return 0;
        }

        @Override
        public PageCursorTracer cursorTracer()
        {
            return NULL;
        }

        @Override
        public CommandsToApply next()
        {
            return null;
        }

        @Override
        public boolean accept( Visitor<StorageCommand,IOException> visitor ) throws IOException
        {
            for ( StorageCommand command : commands )
            {
                visitor.visit( command );
            }
            return false; // <-- false means: all good
        }

        @Override
        public Iterator<StorageCommand> iterator()
        {
            return commands.iterator();
        }
    }

    private static class RelationshipSpec
    {
        private final long id;
        private final long startNodeId;
        private final int type;
        private final long endNodeId;
        private final Set<StorageProperty> properties;

        RelationshipSpec( long startNodeId, int type, long endNodeId, Set<StorageProperty> properties, CommandCreationContext commandCreationContext )
        {
            this( startNodeId, type, endNodeId, properties, commandCreationContext.reserveRelationship( startNodeId ) );
        }

        RelationshipSpec( long startNodeId, int type, long endNodeId, Set<StorageProperty> properties, long id )
        {
            this.startNodeId = startNodeId;
            this.type = type;
            this.endNodeId = endNodeId;
            this.properties = properties;
            this.id = id;
        }

        RelationshipDirection direction( long fromPovOfNodeId )
        {
            return startNodeId == fromPovOfNodeId ? endNodeId == fromPovOfNodeId ? RelationshipDirection.LOOP : RelationshipDirection.OUTGOING
                                                  : RelationshipDirection.INCOMING;
        }

        @Override
        public String toString()
        {
            return "RelationshipSpec{" + "startNodeId=" + startNodeId + ", type=" + type + ", endNodeId=" + endNodeId + ", properties=" + properties + ", id=" +
                    id + '}';
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
            RelationshipSpec that = (RelationshipSpec) o;
            return startNodeId == that.startNodeId && type == that.type && endNodeId == that.endNodeId && Objects.equals( properties, that.properties ) &&
                    id == that.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( id, startNodeId, type, endNodeId, properties );
        }

        void create( TxStateVisitor target )
        {
            try
            {
                target.visitCreatedRelationship( id, type, startNodeId, endNodeId, properties );
            }
            catch ( ConstraintValidationException e )
            {
                throw new RuntimeException( e );
            }
        }

        long neighbourNode( long fromNodeIdPov )
        {
            return startNodeId == fromNodeIdPov ? endNodeId : startNodeId;
        }
    }

    private static class RecordingNodeLabelUpdateListener implements EntityTokenUpdateListener
    {
        private final MutableLongObjectMap<long[]> nodeLabels = LongObjectMaps.mutable.empty();

        @Override
        public void applyUpdates( Iterable<EntityTokenUpdate> labelUpdates, PageCursorTracer cursorTracer )
        {
            for ( EntityTokenUpdate labelUpdate : labelUpdates )
            {
                nodeLabels.put( labelUpdate.getEntityId(), labelUpdate.getTokensAfter() );
            }
        }

        void assertNodeHasLabels( long nodeId, LongSet expectedLabels )
        {
            long[] storedLabels = nodeLabels.get( nodeId );
            assertThat( storedLabels ).isNotNull();
            assertThat( LongSets.immutable.of( storedLabels ) ).isEqualTo( expectedLabels );
        }
    }

    private static class RecordingIndexUpdatesListener extends IndexUpdateListener.Adapter
    {
        private final List<IndexEntryUpdate<IndexDescriptor>> updates = new ArrayList<>();

        @Override
        public void applyUpdates( Iterable<IndexEntryUpdate<IndexDescriptor>> updates, PageCursorTracer cursorTracer )
        {
            updates.forEach( this.updates::add );
        }

        void clear()
        {
            updates.clear();
        }
    }
}
