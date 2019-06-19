/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.core.state.machines.StateMachine;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;

import java.util.Collection;
import java.util.function.Consumer;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.lock.LockGroup;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.token.TokenRegistry;

import static com.neo4j.causalclustering.core.state.machines.token.StorageCommandMarshal.bytesToCommands;
import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterables.single;

public class ReplicatedTokenStateMachine implements StateMachine<ReplicatedTokenRequest>
{
    private TransactionCommitProcess commitProcess;

    private final TokenRegistry tokenRegistry;

    private final Log log;
    private final DatabaseManager<ClusteredDatabaseContext> databaseManager;
    private long lastCommittedIndex = -1;

    public ReplicatedTokenStateMachine( TokenRegistry tokenRegistry, LogProvider logProvider, DatabaseManager<ClusteredDatabaseContext> databaseManager )
    {
        this.tokenRegistry = tokenRegistry;
        this.log = logProvider.getLog( getClass() );
        this.databaseManager = databaseManager;
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

        Collection<StorageCommand> commands = bytesToCommands( tokenRequest.commandBytes() );
        int newTokenId = extractTokenId( commands );
        boolean internal = isInternal( commands );

        String name = tokenRequest.tokenName();
        Integer existingTokenId = internal ? tokenRegistry.getIdInternal( name ) : tokenRegistry.getId( name );

        if ( existingTokenId == null )
        {
            log.info( format( "Applying %s with newTokenId=%d", tokenRequest, newTokenId ) );
            // The 'applyToStore' method applies EXTERNAL transactions, which will update the token holders for us.
            // Thus there is no need for us to update the token registry directly.
            DatabaseId databaseId = tokenRequest.databaseId();
            VersionContextSupplier versionContextSupplier = databaseManager.getDatabaseContext( databaseId ).orElseThrow(
                    () -> new DatabaseNotFoundException( databaseId.name() ) ).database().getVersionContextSupplier();
            applyToStore( commands, commandIndex, versionContextSupplier.getVersionContext() );
            callback.accept( Result.of( newTokenId ) );
        }
        else
        {
            // This should be rare so a warning is in order.
            log.warn( format( "Ignored %s (newTokenId=%d) since it already exists with existingTokenId=%d", tokenRequest, newTokenId, existingTokenId ) );
            callback.accept( Result.of( existingTokenId ) );
        }
    }

    private void applyToStore( Collection<StorageCommand> commands, long logIndex, VersionContext versionContext )
    {
        PhysicalTransactionRepresentation representation = new PhysicalTransactionRepresentation( commands );
        representation.setHeader( LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( logIndex ), 0, 0, 0, 0L, 0L, 0 );

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
        StorageCommand.TokenCommand tokenCommand = getSingleTokenCommand( commands );
        return tokenCommand.tokenId();
    }

    private boolean isInternal( Collection<StorageCommand> commands )
    {
        StorageCommand.TokenCommand tokenCommand = getSingleTokenCommand( commands );
        return tokenCommand.isInternal();
    }

    private StorageCommand.TokenCommand getSingleTokenCommand( Collection<StorageCommand> commands )
    {
        StorageCommand command = single( commands );
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
