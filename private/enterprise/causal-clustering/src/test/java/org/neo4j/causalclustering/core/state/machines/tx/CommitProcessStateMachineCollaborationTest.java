/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import org.junit.Test;

import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

public class CommitProcessStateMachineCollaborationTest
{
    private final String databaseName = DEFAULT_DATABASE_NAME;

    @Test
    public void shouldFailTransactionIfLockSessionChanges()
    {
        // given
        int initialLockSessionId = 23;
        TransactionToApply transactionToApply = new TransactionToApply( physicalTx( initialLockSessionId ) );

        int finalLockSessionId = 24;
        TransactionCommitProcess localCommitProcess = mock( TransactionCommitProcess.class );
        ReplicatedTransactionStateMachine stateMachine =
                new ReplicatedTransactionStateMachine( mock( CommandIndexTracker.class ),
                        lockState( finalLockSessionId ), 16, NullLogProvider.getInstance(),
                        PageCursorTracerSupplier.NULL, EmptyVersionContextSupplier.EMPTY );
        stateMachine.installCommitProcess( localCommitProcess, -1L );

        DirectReplicator<ReplicatedTransaction> replicator = new DirectReplicator<>( stateMachine );
        ReplicatedTransactionCommitProcess commitProcess = new ReplicatedTransactionCommitProcess( replicator, databaseName );

        // when
        try
        {
            commitProcess.commit( transactionToApply, NULL, EXTERNAL );
            fail( "Should have thrown exception." );
        }
        catch ( TransactionFailureException e )
        {
            // expected
        }
    }

    private PhysicalTransactionRepresentation physicalTx( int lockSessionId )
    {
        PhysicalTransactionRepresentation physicalTx = mock( PhysicalTransactionRepresentation.class );
        when( physicalTx.getLockSessionId() ).thenReturn( lockSessionId );
        return physicalTx;
    }

    private ReplicatedLockTokenStateMachine lockState( int lockSessionId )
    {
        ReplicatedLockTokenRequest lockTokenRequest = new ReplicatedLockTokenRequest( null, lockSessionId, databaseName );
        ReplicatedLockTokenStateMachine lockState = mock( ReplicatedLockTokenStateMachine.class );
        when( lockState.snapshot() ).thenReturn( new ReplicatedLockTokenState( -1, lockTokenRequest ) );
        return lockState;
    }
}
