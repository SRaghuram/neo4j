/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.internal.event;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;

class TxStateTransactionDataViewTest
{
    private final ThreadToStatementContextBridge bridge = mock( ThreadToStatementContextBridge.class );
    private final Statement stmt = mock( Statement.class );
    private final StubStorageCursors ops = new StubStorageCursors();
    private final KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    private final InternalTransaction internalTransaction = mock( InternalTransaction.class );
    private final TokenRead tokenRead = mock( TokenRead.class );

    private final TransactionState state = new TxState();

    @BeforeEach
    void setup() throws PropertyKeyIdNotFoundKernelException
    {
        when( transaction.internalTransaction() ).thenReturn( internalTransaction );
        when( transaction.tokenRead() ).thenReturn( tokenRead );
        when( bridge.get( any( DatabaseId.class ) ) ).thenReturn( stmt );
        when( tokenRead.propertyKeyName( anyInt() ) ).thenAnswer( invocationOnMock ->
        {
            int id = invocationOnMock.getArgument( 0 );
            return ops.propertyKeyTokenHolder().getTokenById( id ).name();
        } );
    }

    @Test
    void showsCreatedNodes()
    {
        // Given
        state.nodeDoCreate( 1 );
        state.nodeDoCreate( 2 );

        // When & Then
        assertThat( idList( snapshot().createdNodes() ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    void showsDeletedNodes() throws Exception
    {
        // Given
        state.nodeDoDelete( 1L );
        state.nodeDoDelete( 2L );

        int labelId = 15;
        when( tokenRead.nodeLabelName( labelId ) ).thenReturn( "label" );

        ops.withNode( 1 ).labels( labelId ).properties( "key", Values.of( "p" ) );
        ops.withNode( 2 );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedNodes() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedLabels() ).label().name(), equalTo( "label" ) );
        assertThat( single( snapshot.removedNodeProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    void showsAddedRelationships()
    {
        // Given
        state.relationshipDoCreate( 1, 1, 1L, 2L );
        state.relationshipDoCreate( 2, 1, 1L, 1L );

        // When & Then
        assertThat( idList( snapshot().createdRelationships() ), equalTo( asList( 1L, 2L ) ) );
    }

    @Test
    void showsRemovedRelationships()
    {
        // Given
        state.relationshipDoDelete( 1L, 1, 1L, 2L );
        state.relationshipDoDelete( 2L, 1, 1L, 1L );

        ops.withRelationship( 1, 1, 1, 2 );
        ops.withRelationship( 2, 1, 1, 1 ).properties( "key", Values.of( "p" ) );

        // When & Then
        TxStateTransactionDataSnapshot snapshot = snapshot();
        assertThat( idList( snapshot.deletedRelationships() ), equalTo( asList( 1L, 2L ) ) );
        assertThat( single( snapshot.removedRelationshipProperties() ).key(), equalTo( "key" ) );
    }

    @Test
    void correctlySaysNodeIsDeleted()
    {
        // Given
        state.nodeDoDelete( 1L );
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( 1L );
        ops.withNode( 1 );

        // When & Then
        assertThat( snapshot().isDeleted( node ), equalTo( true ) );
    }

    @Test
    void correctlySaysRelIsDeleted()
    {
        // Given
        state.relationshipDoDelete( 1L, 1, 1L, 2L );

        Relationship rel = mock( Relationship.class );
        when( rel.getId() ).thenReturn( 1L );
        ops.withRelationship( 1L, 1L, 1, 2L );

        // When & Then
        assertThat( snapshot().isDeleted( rel ), equalTo( true ) );
    }

    @Test
    void shouldListAddedNodePropertiesProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyTokenHolder().getOrCreateId( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.nodeDoChangeProperty( 1L, propertyKeyId, Values.of( "newValue" ) );
        ops.withNode( 1 ).properties( "theKey", prevValue );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().assignedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    void shouldListRemovedNodeProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyTokenHolder().getOrCreateId( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.nodeDoRemoveProperty( 1L, propertyKeyId );
        ops.withNode( 1 ).properties( "theKey", prevValue );

        // When
        Iterable<PropertyEntry<Node>> propertyEntries = snapshot().removedNodeProperties();

        // Then
        PropertyEntry<Node> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    void shouldListRemovedRelationshipProperties() throws Exception
    {
        // Given
        int propertyKeyId = ops.propertyKeyTokenHolder().getOrCreateId( "theKey" );
        Value prevValue = Values.of( "prevValue" );
        state.relationshipDoRemoveProperty( 1L, propertyKeyId );
        ops.withRelationship( 1, 0, 0, 0 ).properties( "theKey", prevValue );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().removedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    void shouldListAddedRelationshipProperties() throws Exception
    {
        // Given
        Value prevValue = Values.of( "prevValue" );
        int propertyKeyId = ops.propertyKeyTokenHolder().getOrCreateId( "theKey" );
        state.relationshipDoReplaceProperty( 1L, propertyKeyId, prevValue, Values.of( "newValue" ) );
        ops.withRelationship( 1, 0, 0, 0 ).properties( "theKey", prevValue );

        // When
        Iterable<PropertyEntry<Relationship>> propertyEntries = snapshot().assignedRelationshipProperties();

        // Then
        PropertyEntry<Relationship> entry = single( propertyEntries );
        assertThat( entry.key(), equalTo( "theKey" ) );
        assertThat( entry.value(), equalTo( "newValue" ) );
        assertThat( entry.previouslyCommittedValue(), equalTo( "prevValue" ) );
        assertThat( entry.entity().getId(), equalTo( 1L ) );
    }

    @Test
    void shouldListAddedLabels() throws Exception
    {
        // Given
        int labelId = 2;
        when( tokenRead.nodeLabelName( labelId ) ).thenReturn( "theLabel" );
        state.nodeDoAddLabel( labelId, 1L );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().assignedLabels();

        // Then
        LabelEntry entry = single( labelEntries );
        assertThat( entry.label().name(), equalTo( "theLabel" ) );
        assertThat( entry.node().getId(), equalTo( 1L ) );
    }

    @Test
    void shouldListRemovedLabels() throws Exception
    {
        // Given
        int labelId = 2;
        when( tokenRead.nodeLabelName( labelId ) ).thenReturn( "theLabel" );
        state.nodeDoRemoveLabel( labelId, 1L );

        // When
        Iterable<LabelEntry> labelEntries = snapshot().removedLabels();

        // Then
        LabelEntry entry = single( labelEntries );
        assertThat( entry.label().name(), equalTo( "theLabel" ) );
        assertThat( entry.node().getId(), equalTo( 1L ) );
    }

    @Test
    void accessTransactionIdAndCommitTime()
    {
        long committedTransactionId = 7L;
        long commitTime = 10L;
        when( transaction.getTransactionId() ).thenReturn( committedTransactionId );
        when( transaction.getCommitTime() ).thenReturn( commitTime );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( committedTransactionId, transactionDataSnapshot.getTransactionId() );
        assertEquals( commitTime, transactionDataSnapshot.getCommitTime() );
    }

    @Test
    void shouldGetEmptyUsernameForAnonymousContext()
    {
        when( transaction.securityContext() ).thenReturn( AnonymousContext.read().authorize( LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME ) );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( "", transactionDataSnapshot.username() );
    }

    @Test
    void shouldAccessUsernameFromAuthSubject()
    {
        AuthSubject authSubject = mock( AuthSubject.class );
        when( authSubject.username() ).thenReturn( "Christof" );
        when( transaction.securityContext() )
                .thenReturn( new SecurityContext( authSubject, AccessMode.Static.FULL ) );

        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( "Christof", transactionDataSnapshot.username() );
    }

    @Test
    void shouldAccessEmptyMetaData()
    {
        TxStateTransactionDataSnapshot transactionDataSnapshot = snapshot();
        assertEquals( 0, transactionDataSnapshot.metaData().size() );
    }

    @Test
    void shouldAccessExampleMetaData()
    {
        when( transaction.getMetaData() ).thenReturn( genericMap( "username", "Igor" ) );
        TxStateTransactionDataSnapshot transactionDataSnapshot =
                new TxStateTransactionDataSnapshot( state, ops, transaction );
        assertEquals( 1, transactionDataSnapshot.metaData().size() );
        assertThat( "Expected metadata map to contain defined username", transactionDataSnapshot.metaData(),
                equalTo( genericMap( "username", "Igor" ) ) );
    }

    private static List<Long> idList( Iterable<? extends Entity> entities )
    {
        List<Long> out = new ArrayList<>();
        for ( Entity entity : entities )
        {
            out.add( entity instanceof Node ? entity.getId() : entity.getId() );
        }
        return out;
    }

    private TxStateTransactionDataSnapshot snapshot()
    {
        when( internalTransaction.newNodeProxy( anyLong() ) )
                .thenAnswer( invocation -> new NodeEntity( internalTransaction, invocation.getArgument( 0 ) ) );
        when( internalTransaction.newRelationshipProxy( anyLong() ) )
                .thenAnswer( invocation -> new RelationshipProxy( internalTransaction, invocation.getArgument( 0 ) ) );
        when( internalTransaction.newRelationshipProxy( anyLong(), anyLong(), anyInt(), anyLong() ) )
                .thenAnswer( invocation -> new RelationshipProxy( internalTransaction,
                        invocation.getArgument( 0 ), invocation.getArgument( 1 ),
                        invocation.getArgument( 2 ), invocation.getArgument( 3 ) ) );
        return new TxStateTransactionDataSnapshot( state, ops, transaction );
    }
}
