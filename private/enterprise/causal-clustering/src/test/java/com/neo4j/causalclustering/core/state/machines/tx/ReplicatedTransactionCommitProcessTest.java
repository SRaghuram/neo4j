/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class ReplicatedTransactionCommitProcessTest
{
    private static final NamedDatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();

    private final ClusterLeaseCoordinator leaseCoordinator = mock( ClusterLeaseCoordinator.class );
    private final Replicator replicator = mock( Replicator.class );

    private TransactionRepresentation tx = mock( TransactionRepresentation.class );
    private ReplicatedTransactionCommitProcess commitProcess;

    @BeforeEach
    void tx()
    {
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        commitProcess = new ReplicatedTransactionCommitProcess( replicator, DATABASE_ID, leaseCoordinator, LogEntryWriterFactory.LATEST );
    }

    @Test
    void shouldReturnTransactionIdWhenReplicationSucceeds() throws Exception
    {
        // given
        long expectedTxId = 5;

        ReplicationResult replicationResult = ReplicationResult.applied( StateMachineResult.of( expectedTxId ) );
        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( replicationResult );

        // when
        long txId = commitProcess.commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL, EXTERNAL );

        // then
        assertEquals( expectedTxId, txId );
    }

    @Test
    void shouldThrowTransactionFailureReplicatorFailsWithNotReplicated()
    {
        // given
        RuntimeException replicatorFailure = new RuntimeException();

        ReplicationResult replicationResult = ReplicationResult.notReplicated( replicatorFailure );
        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( replicationResult );

        // when
        TransactionFailureException commitException = assertThrows( TransactionFailureException.class,
                () -> commitProcess.commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL, EXTERNAL ) );

        // then
        assertEquals( replicatorFailure, commitException.getCause() );
    }

    @Test
    void shouldThrowTransactionFailureAndInvalidateLeaseReplicatorFailsWithMaybeReplicated()
    {
        // given
        RuntimeException replicatorFailure = new RuntimeException();
        ReplicationResult replicationResult = ReplicationResult.maybeReplicated( replicatorFailure );

        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenReturn( replicationResult );

        // when
        TransactionFailureException commitException = assertThrows( TransactionFailureException.class,
                () -> commitProcess.commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL, EXTERNAL ) );

        // then
        assertEquals( replicatorFailure, commitException.getCause() );
        verify( leaseCoordinator ).invalidateLease( anyInt() );
    }

    @Test
    void shouldThrowTransactionFailureAndInvalidateLeaseWhenReplicatorFailsExceptionally()
    {
        // given
        RuntimeException replicatorException = new RuntimeException();
        when( replicator.replicate( any( ReplicatedContent.class ) ) ).thenThrow( replicatorException );

        // when
        TransactionFailureException commitException = assertThrows( TransactionFailureException.class,
                () -> commitProcess.commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL, EXTERNAL ) );

        // then
        assertEquals( replicatorException, commitException.getCause() );
        verify( leaseCoordinator ).invalidateLease( anyInt() );
    }
}
