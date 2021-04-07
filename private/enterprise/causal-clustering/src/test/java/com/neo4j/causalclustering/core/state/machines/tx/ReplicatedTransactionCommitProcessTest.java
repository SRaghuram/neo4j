/*
 * Copyright (c) "Neo4j"
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

import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class ReplicatedTransactionCommitProcessTest
{
    private static final NamedDatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase();

    private final ClusterLeaseCoordinator leaseCoordinator = mock( ClusterLeaseCoordinator.class );
    private final Replicator replicator = mock( Replicator.class );

    private final TransactionRepresentation tx = mock( TransactionRepresentation.class );
    private ReplicatedTransactionCommitProcess commitProcess;

    @BeforeEach
    void tx()
    {
        when( tx.additionalHeader() ).thenReturn( new byte[]{} );
        commitProcess = new ReplicatedTransactionCommitProcess( replicator, DATABASE_ID, leaseCoordinator,
                                                                LogEntryWriterFactory.LATEST, writable() );
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
                                                                    () -> commitProcess
                                                                            .commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL,
                                                                                     EXTERNAL ) );

        // then
        assertEquals( replicatorException, commitException.getCause() );
        verify( leaseCoordinator ).invalidateLease( anyInt() );
    }

    @Test
    void shouldFailIfDatabaseIsReadOnly()
    {
        //given
        var config = Config.defaults( read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );
        var readOnlyChecker = new DatabaseReadOnlyChecker.Default( config, DEFAULT_DATABASE_NAME );
        final var commitProcess =
                new ReplicatedTransactionCommitProcess( replicator, DATABASE_ID, leaseCoordinator, LogEntryWriterFactory.LATEST, readOnlyChecker );

        // when
        final var exception = assertThrows( RuntimeException.class,
                                            () -> commitProcess.commit( new TransactionToApply( tx, PageCursorTracer.NULL ), CommitEvent.NULL, EXTERNAL ) );
        assertThat( getRootCause( exception ).getMessage() )
                  .contains( "This Neo4j instance is read only for the database " + DEFAULT_DATABASE_NAME );
    }
}
