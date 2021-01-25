/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.DirectReplicator;
import com.neo4j.causalclustering.core.state.machines.DummyStateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.lease.ClusterLeaseCoordinator;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseStateMachine;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.NullLogProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

class CommitProcessStateMachineCollaborationTest
{
    private final NamedDatabaseId nameDatabaseId = new TestDatabaseIdRepository().defaultDatabase();

    @Test
    void shouldFailTransactionIfLeaseChanges()
    {
        // given
        int initialLeaseId = 23;
        TransactionToApply transactionToApply = new TransactionToApply( physicalTx( initialLeaseId ), PageCursorTracer.NULL );

        int finalLeaseId = 24;
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine( new DummyStateMachineCommitHelper(),
                leaseState( finalLeaseId ), 16, NullLogProvider.getInstance(), new TestCommandReaderFactory() );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        DirectReplicator<ReplicatedTransaction> replicator = new DirectReplicator<>( stateMachine );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, nameDatabaseId,
                                                                                                   mock( ClusterLeaseCoordinator.class ),
                                                                                                   LogEntryWriterFactory.LATEST,
                                                                                                   ReadOnlyDatabaseChecker.neverReadOnly() );

        // when
        assertThrows( TransactionFailureException.class, () -> commitProcess.commit( transactionToApply, NULL, EXTERNAL ) );
    }

    private static PhysicalTransactionRepresentation physicalTx( int leaseSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLeaseId() ).thenReturn( leaseSessionId );
        return physicalTx;
    }

    private ReplicatedLeaseStateMachine leaseState( int leaseId )
    {
        ReplicatedLeaseRequest leaseRequest = new ReplicatedLeaseRequest( null, leaseId, nameDatabaseId.databaseId() );
        ReplicatedLeaseStateMachine leaseState = mock( ReplicatedLeaseStateMachine.class );
        when( leaseState.snapshot() ).thenReturn( new ReplicatedLeaseState( -1, leaseRequest ) );
        return leaseState;
    }
}
