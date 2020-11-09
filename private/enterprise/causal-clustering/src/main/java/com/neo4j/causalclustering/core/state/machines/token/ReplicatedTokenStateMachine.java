/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.machines.StateMachineCommitHelper;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;

import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.token.TokenRegistry;

import static com.neo4j.causalclustering.core.state.machines.token.StorageCommandMarshal.bytesToCommands;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;

public class ReplicatedTokenStateMachine implements StateMachine<ReplicatedTokenRequest>
{
    private final StateMachineCommitHelper commitHelper;
    private final TokenRegistry tokenRegistry;
    private final Log log;
    private final CommandReaderFactory commandReaderFactory;

    private TransactionCommitProcess commitProcess;
    private long lastCommittedIndex = -1;

    public ReplicatedTokenStateMachine( StateMachineCommitHelper commitHelper, TokenRegistry tokenRegistry, LogProvider logProvider,
            CommandReaderFactory commandReaderFactory )
    {
        this.commitHelper = commitHelper;
        this.tokenRegistry = tokenRegistry;
        this.log = logProvider.getLog( getClass() );
        this.commandReaderFactory = commandReaderFactory;
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.commitProcess = commitProcess;
        this.lastCommittedIndex = lastCommittedIndex;
        commitHelper.updateLastAppliedCommandIndex( lastCommittedIndex );
        log.info( format("(%s) Updated lastCommittedIndex to %d", tokenRegistry.getTokenType(), lastCommittedIndex) );
    }

    @Override
    public synchronized void applyCommand( ReplicatedTokenRequest tokenRequest, long commandIndex,
            Consumer<StateMachineResult> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.warn( format( "Ignored %s because already committed (%d <= %d).", tokenRequest, commandIndex, lastCommittedIndex ) );
            return;
        }

        var commands = bytesToCommands( tokenRequest.commandBytes(), commandReaderFactory );
        var newTokenId = extractTokenId( commands );
        var internal = isInternal( commands );

        var name = tokenRequest.tokenName();
        var existingTokenId = internal ? tokenRegistry.getIdInternal( name ) : tokenRegistry.getId( name );
        if ( existingTokenId != null )
        {
            // This should be rare so a warning is in order.
            log.warn( format( "Ignored %s (newTokenId=%d) since it already exists with existingTokenId=%d", tokenRequest, newTokenId, existingTokenId ) );
            callback.accept( StateMachineResult.of( existingTokenId ) );
            return;
        }

        var existingToken = internal ? tokenRegistry.getTokenInternal( newTokenId ) : tokenRegistry.getToken( newTokenId );
        if ( existingToken != null )
        {
            log.warn( "Token id %d already exists for %s. Cannot be added to %s", newTokenId, existingToken, name );
            callback.accept( StateMachineResult.of(
                    new TransientTransactionFailureException( Status.Transaction.Outdated, "Token registry is out of date. Try again." ) ) );
            return;
        }

        log.info( format( "Applying %s with newTokenId=%d", tokenRequest, newTokenId ) );
        // The 'applyToStore' method applies EXTERNAL transactions, which will update the token holders for us.
        // Thus there is no need for us to update the token registry directly.
        applyToStore( commands, commandIndex );
        callback.accept( StateMachineResult.of( newTokenId ) );
    }

    private void applyToStore( Collection<StorageCommand> commands, long logIndex )
    {
        var representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( logIndex ), 0, 0L, 0L, 0, AUTH_DISABLED );

        try
        {
            commitHelper.commit( commitProcess, representation, logIndex );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int extractTokenId( Collection<StorageCommand> commands )
    {
        var tokenCommand = getSingleTokenCommand( commands );
        return tokenCommand.tokenId();
    }

    private boolean isInternal( Collection<StorageCommand> commands )
    {
        var tokenCommand = getSingleTokenCommand( commands );
        return tokenCommand.isInternal();
    }

    private StorageCommand.TokenCommand getSingleTokenCommand( Collection<StorageCommand> commands )
    {
        var command = single( commands );
        if ( !( command instanceof StorageCommand.TokenCommand ) )
        {
            throw new IllegalArgumentException( "Was expecting a single token command, got " + command );
        }
        return (StorageCommand.TokenCommand) command;
    }

    @Override
    public synchronized void flush()
    {
        // already implicitly flushed to the store
    }

    @Override
    public long lastAppliedIndex()
    {
        if ( commitProcess == null )
        {
            /* See {@link #installCommitProcess}. */
            throw new IllegalStateException( "Value has not been installed" );
        }
        return lastCommittedIndex;
    }
}
