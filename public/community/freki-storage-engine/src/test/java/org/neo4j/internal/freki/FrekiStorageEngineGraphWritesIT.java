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
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaState;
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
import org.neo4j.monitoring.DatabaseEventListeners;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.DatabasePanicEventGenerator;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdate;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.PropertyKeyValue;
import org.neo4j.storageengine.api.StandardConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
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
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.lock.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;
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
    private StorageEngine storageEngine;
    private RecordingNodeLabelUpdateListener nodeLabelUpdateListener;
    private RecordingIndexUpdatesListener indexUpdateListener;
    private CommandCreationContext commandCreationContext;

    @BeforeEach
    void start() throws IOException
    {
        FrekiStorageEngineFactory factory = new FrekiStorageEngineFactory();
        DatabaseLayout layout = Neo4jLayout.of( directory.homeDir() ).databaseLayout( DEFAULT_DATABASE_NAME );
        fs.mkdirs( layout.databaseDirectory() );
        TokenHolders tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        DatabaseHealth databaseHealth = new DatabaseHealth( new DatabasePanicEventGenerator(
                new DatabaseEventListeners( NullLog.getInstance() ), DEFAULT_DATABASE_NAME ), NullLog.getInstance() );
        life = new LifeSupport();
        storageEngine = life.add(
                factory.instantiate( fs, layout, Config.defaults(), pageCache, tokenHolders, mock( SchemaState.class ), new StandardConstraintRuleAccessor(),
                        new NoopIndexConfigCompletor(), NO_LOCK_SERVICE, new DefaultIdGeneratorFactory( fs, RecoveryCleanupWorkCollector.immediate() ),
                        new DefaultIdController(), databaseHealth, NullLogProvider.getInstance(), RecoveryCleanupWorkCollector.immediate(),
                        PageCacheTracer.NULL, true ) );
        nodeLabelUpdateListener = new RecordingNodeLabelUpdateListener();
        storageEngine.addNodeLabelUpdateListener( nodeLabelUpdateListener );
        indexUpdateListener = new RecordingIndexUpdatesListener();
        storageEngine.addIndexUpdateListener( indexUpdateListener );
        life.start();
        commandCreationContext = storageEngine.newCommandCreationContext( NULL );
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
        Set<RelationshipSpec> relationships = asSet( new RelationshipSpec( nodeId1, 99, nodeId2, relationshipProperties ) );

        // when
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId1 );
            target.visitNodeLabelChanges( nodeId1, labelIds, LongSets.immutable.empty() );
            target.visitCreatedNode( nodeId2 );
            target.visitNodePropertyChanges( nodeId1, nodeProperties, Collections.emptyList(), IntSets.immutable.empty() );
            target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId1 ), 99, nodeId1, nodeId2, relationshipProperties );
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
        Set<RelationshipSpec> relationships = asSet( new RelationshipSpec( nodeId1, 99, nodeId2, relationshipProperties ) );
        createAndApplyTransaction( target ->
        {
            LongSet addedLabels = LongSets.immutable.of( 3, 6 );
            labelIds.addAll( addedLabels );
            target.visitNodeLabelChanges( nodeId1, addedLabels, LongSets.immutable.empty() );
            Set<StorageProperty> addedNodeProperties = asSet( new PropertyKeyValue( 2, longValue( 202 ) ) );
            target.visitNodePropertyChanges( nodeId1, addedNodeProperties, Collections.emptyList(), IntSets.immutable.empty() );
            nodeProperties.addAll( addedNodeProperties );
            for ( RelationshipSpec relationship : relationships )
            {
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( relationship.startNodeId ), relationship.type,
                        relationship.startNodeId, relationship.endNodeId, relationshipProperties );
            }
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
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), type, nodeId, otherNodeId, emptyList() );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, emptySet() ) );
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
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), type, nodeId, otherNodeId, emptyList() );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, emptySet() ) );
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
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), type, nodeId, otherNodeId, emptyList() );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, emptySet() ) );
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
                target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), type, nodeId, otherNodeId, relationshipProperties );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, relationshipProperties ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labels, nodeProperties, relationships );
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
                    target.visitCreatedRelationship( commandCreationContext.reserveRelationship( nodeId ), 0, nodeId, otherNodeId, emptyList() );
                    relationships.add( new RelationshipSpec( nodeId, 0, otherNodeId, emptySet() ) );
                }
                PropertyKeyValue property = new PropertyKeyValue( nextPropertyKey.getAndIncrement(), intValue( 1010 ) );
                target.visitNodePropertyChanges( nodeId, singletonList( property ), emptyList(), IntSets.immutable.empty() );
                nodeProperties.add( property );
            } );
        }

        // then
        assertContentsOfNode( nodeId, labels, nodeProperties, relationships );
    }

    // TODO shouldUpdateDenseNode

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
                for ( int j = 0; j < nodes.length; j++ )
                {
                    long startNode = nodes[random.nextInt( nodes.length )];
                    long endNode = nodes[random.nextInt( nodes.length )];
                    target.visitCreatedRelationship( commandCreationContext.reserveRelationship( startNode ), 0, startNode, endNode, emptyList() );
                    RelationshipSpec spec = new RelationshipSpec( startNode, 0, endNode, emptySet() );
                    expectedRelationships.getIfAbsentPut( startNode, HashSet::new ).add( spec );
                    expectedRelationships.getIfAbsentPut( endNode, HashSet::new ).add( spec );
                }
            } );
        }

        // then
        for ( long node : nodes )
        {
            assertContentsOfNode( node, LongSets.immutable.empty(), emptySet(), expectedRelationships.get( node ) );
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
                RelationshipSpec relationshipSpec = new RelationshipSpec( outgoing ? nodeId1 : nodeId2, random.nextInt( 3 ), outgoing ? nodeId2 : nodeId1,
                        asSet( new PropertyKeyValue( 0, intValue( i ) ) ) );
                relationships.add( relationshipSpec );
                long id = commandCreationContext.reserveRelationship( relationshipSpec.startNodeId );
                target.visitCreatedRelationship( id, relationshipSpec.type,
                        relationshipSpec.startNodeId, relationshipSpec.endNodeId, relationshipSpec.properties );
                relationshipSpec.id = id;
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
        }, target ->
        {
            target.visitNodePropertyChanges( nodeId, emptyList(), singleton( new PropertyKeyValue( SCHEMA_DESCRIPTOR.getPropertyId(), afterValue ) ),
                    IntSets.immutable.empty() );
        }, index -> asSet( IndexEntryUpdate.change( nodeId, index, new Value[]{beforeValue}, new Value[]{afterValue} ) ) );
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
        assertThat( Iterables.asSet( indexUpdateListener.updates ) ).isEqualTo( expectedUpdates.apply( index ) );
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
                StoragePropertyCursor propertyCursor = storageReader.allocatePropertyCursor( NULL );
                StorageRelationshipTraversalCursor relationshipCursor = storageReader.allocateRelationshipTraversalCursor( NULL ) )
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
            Set<RelationshipSpec> readRelationships = new HashSet<>();
            while ( relationshipCursor.next() )
            {
                relationshipCursor.properties( propertyCursor );
                RelationshipSpec relationship =
                        new RelationshipSpec( relationshipCursor.sourceNodeReference(), relationshipCursor.type(), relationshipCursor.targetNodeReference(),
                                readProperties( propertyCursor ) );
                readRelationships.add( relationship );
            }
            assertThat( readRelationships ).isEqualTo( relationships );
        }
    }

    private void assertProperties( Set<StorageProperty> expectedProperties, StoragePropertyCursor propertyCursor )
    {
        Set<StorageProperty> readNodeProperties = readProperties( propertyCursor );
        assertThat( readNodeProperties ).isEqualTo( expectedProperties );
    }

    private Set<StorageProperty> readProperties( StoragePropertyCursor propertyCursor )
    {
        Set<StorageProperty> readNodeProperties = new HashSet<>();
        while ( propertyCursor.next() )
        {
            assertThat( readNodeProperties.add( new PropertyKeyValue( propertyCursor.propertyKey(), propertyCursor.propertyValue() ) ) ).isTrue();
        }
        return readNodeProperties;
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
        private final long startNodeId;
        private final int type;
        private final long endNodeId;
        private final Set<StorageProperty> properties;

        private long id;

        RelationshipSpec( long startNodeId, int type, long endNodeId, Set<StorageProperty> properties )
        {
            this.startNodeId = startNodeId;
            this.type = type;
            this.endNodeId = endNodeId;
            this.properties = properties;
        }

        @Override
        public String toString()
        {
            return "RelationshipSpec{" + "startNodeId=" + startNodeId + ", type=" + type + ", endNodeId=" + endNodeId + ", properties=" + properties + '}';
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
            return startNodeId == that.startNodeId && type == that.type && endNodeId == that.endNodeId && Objects.equals( properties, that.properties );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( startNodeId, type, endNodeId, properties );
        }
    }

    private static class RecordingNodeLabelUpdateListener implements NodeLabelUpdateListener
    {
        private final MutableLongObjectMap<long[]> nodeLabels = LongObjectMaps.mutable.empty();

        @Override
        public void applyUpdates( Iterable<NodeLabelUpdate> labelUpdates )
        {
            for ( NodeLabelUpdate labelUpdate : labelUpdates )
            {
                nodeLabels.put( labelUpdate.getNodeId(), labelUpdate.getLabelsAfter() );
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
