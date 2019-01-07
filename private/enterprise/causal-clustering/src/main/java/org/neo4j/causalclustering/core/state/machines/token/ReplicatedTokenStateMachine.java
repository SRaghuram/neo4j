/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.StateMachine;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.String.format;
import static org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequestSerializer.extractCommands;
import static org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader;

public class ReplicatedTokenStateMachine implements StateMachine<ReplicatedTokenRequest>
{
    private TransactionCommitProcess commitProcess;

    private final TokenRegistry tokenRegistry;
    private final VersionContext versionContext;

    private final Log log;
    private long lastCommittedIndex = -1;

    public ReplicatedTokenStateMachine( TokenRegistry tokenRegistry,
            LogProvider logProvider, VersionContextSupplier versionContextSupplier )
    {
        this.tokenRegistry = tokenRegistry;
        this.versionContext = versionContextSupplier.getVersionContext();
        this.log = logProvider.getLog( getClass() );
    }

    public synchronized void installCommitProcess( TransactionCommitProcess commitProcess, long lastCommittedIndex )
    {
        this.commitProcess = commitProcess;
        this.lastCommittedIndex = lastCommittedIndex;
        log.info( format("(%s) Updated lastCommittedIndex to %d", tokenRegistry.getTokenType(), lastCommittedIndex) );
    }

    @Override
    public synchronized void applyCommand( ReplicatedTokenRequest tokenRequest, long commandIndex,
            Consumer<Result> callback )
    {
        if ( commandIndex <= lastCommittedIndex )
        {
            log.warn( format( "Ignored %s because already committed (%d <= %d).", tokenRequest, commandIndex, lastCommittedIndex ) );
            return;
        }

        Collection<StorageCommand> commands = extractCommands( tokenRequest.commandBytes() );
        int newTokenId = extractTokenId( commands );

        Integer existingTokenId = tokenRegistry.getId( tokenRequest.tokenName() );

        if ( existingTokenId == null )
        {
            log.info( format( "Applying %s with newTokenId=%d", tokenRequest, newTokenId ) );
            applyToStore( commands, commandIndex );
            tokenRegistry.put( new NamedToken( tokenRequest.tokenName(), newTokenId ) );
            callback.accept( Result.of( newTokenId ) );
        }
        else
        {
            // This should be rare so a warning is in order.
            log.warn( format( "Ignored %s (newTokenId=%d) since it already exists with existingTokenId=%d", tokenRequest, newTokenId, existingTokenId ) );
            callback.accept( Result.of( existingTokenId ) );
        }
    }

    private void applyToStore( Collection<StorageCommand> commands, long logIndex )
    {
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( encodeLogIndexAsTxHeader( logIndex ), 0, 0, 0, 0L, 0L, 0 );

        try ( LockGroup ignored = new LockGroup() )
        {
            commitProcess.commit( new TransactionToApply( representation, versionContext ), CommitEvent.NULL,
                    TransactionApplicationMode.EXTERNAL );
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int extractTokenId( Collection<StorageCommand> commands )
    {
        for ( StorageCommand command : commands )
        {
            if ( command instanceof Command.TokenCommand )
            {
                return ((Command.TokenCommand<? extends TokenRecord>) command).getAfter().getIntId();
            }
        }
        throw new IllegalStateException( "Commands did not contain token command" );
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
