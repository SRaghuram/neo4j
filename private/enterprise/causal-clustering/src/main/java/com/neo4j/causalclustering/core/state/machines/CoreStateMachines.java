/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseRequest;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseStateMachine;
import com.neo4j.causalclustering.core.state.machines.status.StatusRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.kernel.impl.api.TransactionCommitProcess;

public class CoreStateMachines
{
    private final ReplicatedTransactionStateMachine replicatedTxStateMachine;

    private final ReplicatedTokenStateMachine labelTokenStateMachine;
    private final ReplicatedTokenStateMachine relationshipTypeTokenStateMachine;
    private final ReplicatedTokenStateMachine propertyKeyTokenStateMachine;
    private final ReplicatedLeaseStateMachine replicatedLeaseStateMachine;
    private final NoOperationStateMachine<NoOperationRequest> noOperationStateMachine;

    private final RecoverConsensusLogIndex consensusLogIndexRecovery;

    private final CommandDispatcher dispatcher;

    public CoreStateMachines(
            ReplicatedTransactionStateMachine replicatedTxStateMachine,
            ReplicatedTokenStateMachine labelTokenStateMachine,
            ReplicatedTokenStateMachine relationshipTypeTokenStateMachine,
            ReplicatedTokenStateMachine propertyKeyTokenStateMachine,
            ReplicatedLeaseStateMachine replicatedLeaseStateMachine,
            RecoverConsensusLogIndex consensusLogIndexRecovery,
            NoOperationStateMachine<NoOperationRequest> noOperationStateMachine )
    {
        this.replicatedTxStateMachine = replicatedTxStateMachine;
        this.labelTokenStateMachine = labelTokenStateMachine;
        this.relationshipTypeTokenStateMachine = relationshipTypeTokenStateMachine;
        this.propertyKeyTokenStateMachine = propertyKeyTokenStateMachine;
        this.replicatedLeaseStateMachine = replicatedLeaseStateMachine;
        this.noOperationStateMachine = noOperationStateMachine;
        this.consensusLogIndexRecovery = consensusLogIndexRecovery;
        this.dispatcher = new StateMachineCommandDispatcher();
    }

    public CommandDispatcher commandDispatcher()
    {
        return dispatcher;
    }

    public long getLastAppliedIndex()
    {
        return replicatedLeaseStateMachine.lastAppliedIndex();
    }

    public void flush() throws IOException
    {
        replicatedTxStateMachine.flush();

        labelTokenStateMachine.flush();
        relationshipTypeTokenStateMachine.flush();
        propertyKeyTokenStateMachine.flush();

        replicatedLeaseStateMachine.flush();
    }

    public void augmentSnapshot( CoreSnapshot coreSnapshot )
    {
        coreSnapshot.add( CoreStateFiles.LEASE, replicatedLeaseStateMachine.snapshot() );
        // transactions and tokens live in the store
    }

    public void installSnapshot( CoreSnapshot coreSnapshot )
    {
        replicatedLeaseStateMachine.installSnapshot( coreSnapshot.get( CoreStateFiles.LEASE ) );
        // transactions and tokens live in the store
    }

    public void installCommitProcess( TransactionCommitProcess localCommit )
    {
        long lastAppliedIndex = consensusLogIndexRecovery.findLastAppliedIndex();

        replicatedTxStateMachine.installCommitProcess( localCommit, lastAppliedIndex );

        labelTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        relationshipTypeTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
        propertyKeyTokenStateMachine.installCommitProcess( localCommit, lastAppliedIndex );
    }

    private class StateMachineCommandDispatcher implements CommandDispatcher
    {
        @Override
        public void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<StateMachineResult> callback )
        {
            replicatedTxStateMachine.applyCommand( transaction, commandIndex, callback );
        }

        @Override
        public void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<StateMachineResult> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            switch ( tokenRequest.type() )
            {
            case PROPERTY:
                propertyKeyTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            case RELATIONSHIP:
                relationshipTypeTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            case LABEL:
                labelTokenStateMachine.applyCommand( tokenRequest, commandIndex, callback );
                break;
            default:
                throw new IllegalStateException();
            }
        }

        @Override
        public void dispatch( ReplicatedLeaseRequest leaseRequest, long commandIndex, Consumer<StateMachineResult> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            replicatedLeaseStateMachine.applyCommand( leaseRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<StateMachineResult> callback )
        {
            noOperationStateMachine.applyCommand( dummyRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( StatusRequest statusRequest, long commandIndex, Consumer<StateMachineResult> callback )
        {
            noOperationStateMachine.applyCommand( statusRequest, commandIndex, callback );
        }

        @Override
        public void close()
        {
            replicatedTxStateMachine.ensuredApplied();
        }
    }
}
