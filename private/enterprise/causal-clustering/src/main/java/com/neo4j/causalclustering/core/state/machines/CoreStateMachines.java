/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.kernel.impl.api.TransactionCommitProcess;

import static java.lang.Math.max;

public class CoreStateMachines
{
    private final ReplicatedTransactionStateMachine replicatedTxStateMachine;

    private final ReplicatedTokenStateMachine labelTokenStateMachine;
    private final ReplicatedTokenStateMachine relationshipTypeTokenStateMachine;
    private final ReplicatedTokenStateMachine propertyKeyTokenStateMachine;
    private final ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine;
    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine;
    private final DummyMachine benchmarkMachine;

    private final RecoverConsensusLogIndex consensusLogIndexRecovery;

    private final CommandDispatcher dispatcher;

    public CoreStateMachines(
            ReplicatedTransactionStateMachine replicatedTxStateMachine,
            ReplicatedTokenStateMachine labelTokenStateMachine,
            ReplicatedTokenStateMachine relationshipTypeTokenStateMachine,
            ReplicatedTokenStateMachine propertyKeyTokenStateMachine,
            ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine,
            ReplicatedIdAllocationStateMachine idAllocationStateMachine,
            DummyMachine benchmarkMachine,
            RecoverConsensusLogIndex consensusLogIndexRecovery )
    {
        this.replicatedTxStateMachine = replicatedTxStateMachine;
        this.labelTokenStateMachine = labelTokenStateMachine;
        this.relationshipTypeTokenStateMachine = relationshipTypeTokenStateMachine;
        this.propertyKeyTokenStateMachine = propertyKeyTokenStateMachine;
        this.replicatedLockTokenStateMachine = replicatedLockTokenStateMachine;
        this.idAllocationStateMachine = idAllocationStateMachine;
        this.benchmarkMachine = benchmarkMachine;
        this.consensusLogIndexRecovery = consensusLogIndexRecovery;
        this.dispatcher = new StateMachineCommandDispatcher();
    }

    public CommandDispatcher commandDispatcher()
    {
        return dispatcher;
    }

    public long getLastAppliedIndex()
    {
        long lastAppliedLockTokenIndex = replicatedLockTokenStateMachine.lastAppliedIndex();
        long lastAppliedIdAllocationIndex = idAllocationStateMachine.lastAppliedIndex();
        return max( lastAppliedLockTokenIndex, lastAppliedIdAllocationIndex );
    }

    public void flush() throws IOException
    {
        replicatedTxStateMachine.flush();

        labelTokenStateMachine.flush();
        relationshipTypeTokenStateMachine.flush();
        propertyKeyTokenStateMachine.flush();

        replicatedLockTokenStateMachine.flush();
        idAllocationStateMachine.flush();
    }

    public void augmentSnapshot( CoreSnapshot coreSnapshot )
    {
        coreSnapshot.add( CoreStateFiles.ID_ALLOCATION, idAllocationStateMachine.snapshot() );
        coreSnapshot.add( CoreStateFiles.LOCK_TOKEN, replicatedLockTokenStateMachine.snapshot() );
        // transactions and tokens live in the store
    }

    public void installSnapshot( CoreSnapshot coreSnapshot )
    {
        idAllocationStateMachine.installSnapshot( coreSnapshot.get( CoreStateFiles.ID_ALLOCATION ) );
        replicatedLockTokenStateMachine.installSnapshot( coreSnapshot.get( CoreStateFiles.LOCK_TOKEN ) );
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
        public void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.applyCommand( transaction, commandIndex, callback );
        }

        @Override
        public void dispatch( ReplicatedIdAllocationRequest idRequest, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            idAllocationStateMachine.applyCommand( idRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback )
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
        public void dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex, Consumer<Result> callback )
        {
            replicatedTxStateMachine.ensuredApplied();
            replicatedLockTokenStateMachine.applyCommand( lockRequest, commandIndex, callback );
        }

        @Override
        public void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback )
        {
            benchmarkMachine.applyCommand( dummyRequest, commandIndex, callback );
        }

        @Override
        public void close()
        {
            replicatedTxStateMachine.ensuredApplied();
        }
    }
}
