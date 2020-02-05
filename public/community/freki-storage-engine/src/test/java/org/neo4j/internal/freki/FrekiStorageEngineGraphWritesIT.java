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

import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.IndexDescriptor;
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
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
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
public class FrekiStorageEngineGraphWritesIT
{
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    private LifeSupport life;
    private StorageEngine storageEngine;

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
        life.start();
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
            target.visitCreatedRelationship( -1, 99, nodeId1, nodeId2, relationshipProperties );
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
                target.visitCreatedRelationship( -1, relationship.type, relationship.startNodeId, relationship.endNodeId, relationshipProperties );
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
                target.visitCreatedRelationship( -1, type, nodeId, otherNodeId, emptyList() );
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
        CommandCreationContext commandCreationContext = storageEngine.newCommandCreationContext( NULL );
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
                target.visitCreatedRelationship( -1, type, nodeId, otherNodeId, emptyList() );
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
                target.visitCreatedRelationship( -1, type, nodeId, otherNodeId, emptyList() );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, emptySet() ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labelIds, nodeProperties, relationships );
    }

    @Test
    void shouldOverflowIntoDenseRepresentationFor() throws Exception
    {
        // given
        CommandCreationContext commandCreationContext = storageEngine.newCommandCreationContext( NULL );
        long nodeId = commandCreationContext.reserveNode();
        ImmutableLongSet labels = LongSets.immutable.of( 1, 78, 95 );
        int type = 12;

        // when
        Set<RelationshipSpec> relationships = new HashSet<>();
        createAndApplyTransaction( target ->
        {
            target.visitCreatedNode( nodeId );
            target.visitNodeLabelChanges( nodeId, labels, LongSets.immutable.empty() );
            for ( int i = 0; i < 1_000; i++ )
            {
                long otherNodeId = commandCreationContext.reserveNode();
                target.visitCreatedNode( otherNodeId );
                target.visitCreatedRelationship( i, type, nodeId, otherNodeId, emptyList() );
                relationships.add( new RelationshipSpec( nodeId, type, otherNodeId, emptySet() ) );
            }
        } );

        // then
        assertContentsOfNode( nodeId, labels, emptySet(), relationships );
    }

    @Test
    void shouldUpdateDenseNode()
    {
        // given

        // when

        // then
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
    }

    private Collection<StorageCommand> createCommands( ThrowingConsumer<TxStateVisitor,Exception> data ) throws KernelException
    {
        Collection<StorageCommand> commands = new ArrayList<>();
        try ( StorageReader reader = storageEngine.newReader() )
        {
            ReadableTransactionState transactionState = mock( ReadableTransactionState.class );
            doAnswer( invocationOnMock ->
            {
                TxStateVisitor visitor = invocationOnMock.getArgument( 0, TxStateVisitor.class );
                data.accept( visitor );
                return null;
            } ).when( transactionState ).accept( any() );
            storageEngine.createCommands( commands, transactionState, reader, storageEngine.newCommandCreationContext( NULL ),
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
}
