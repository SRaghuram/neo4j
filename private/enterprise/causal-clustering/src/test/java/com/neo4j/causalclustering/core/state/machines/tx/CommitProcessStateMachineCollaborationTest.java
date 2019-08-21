/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.DirectReplicator;
import com.neo4j.causalclustering.core.state.machines.DummyStateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenRequest;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenState;
import com.neo4j.causalclustering.core.state.machines.barrier.ReplicatedBarrierTokenStateMachine;
import com.neo4j.causalclustering.error_handling.DatabasePanicker;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
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
    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase();

    @Test
    void shouldFailTransactionIfLockSessionChanges()
    {
        // given
        int initialLockSessionId = 23;
        TransactionToApply transactionToApply = new TransactionToApply( physicalTx( initialLockSessionId ) );

        int finalLockSessionId = 24;
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine stateMachine = new ReplicatedTransactionStateMachine( new DummyStateMachineCommitHelper(),
                lockState( finalLockSessionId ), 16, NullLogProvider.getInstance() );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        DirectReplicator<ReplicatedTransaction> replicator = new DirectReplicator<>( stateMachine );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, databaseId, mock( DatabasePanicker.class ) );

        // when
        assertThrows( TransactionFailureException.class, () -> commitProcess.commit( transactionToApply, NULL, EXTERNAL ) );
    }

    private static PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getEpochTokenId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    private ReplicatedBarrierTokenStateMachine lockState( int lockSessionId )
    {
        ReplicatedBarrierTokenRequest lockTokenRequest = new ReplicatedBarrierTokenRequest( null, lockSessionId, databaseId );
        ReplicatedBarrierTokenStateMachine lockState = mock( ReplicatedBarrierTokenStateMachine.class );
        when( lockState.snapshot() ).thenReturn( new ReplicatedBarrierTokenState( -1, lockTokenRequest ) );
        return lockState;
    }
}
