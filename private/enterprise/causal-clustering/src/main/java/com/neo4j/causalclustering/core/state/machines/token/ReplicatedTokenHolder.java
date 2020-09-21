/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.replication.Replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.AbstractTokenHolderBase;
import org.neo4j.token.TokenRegistry;

import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

public class ReplicatedTokenHolder extends AbstractTokenHolderBase
{
    private static final String REPLICATED_TOKEN_HOLDER_CREATOR_TAG = "replicatedTokenHolderCreator";
    private final Replicator replicator;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType tokenIdType;
    private final TokenType type;
    private final Supplier<StorageEngine> storageEngineSupplier;
    private final ReplicatedTokenCreator tokenCreator;
    private final NamedDatabaseId namedDatabaseId;
    private final PageCacheTracer pageCacheTracer;
    private final LogEntryWriterFactory logEntryWriterFactory;
    private final ReadOnlyDatabaseChecker readOnlyDatabaseChecker;

    ReplicatedTokenHolder( NamedDatabaseId namedDatabaseId, TokenRegistry tokenRegistry, Replicator replicator,
                           IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
                           Supplier<StorageEngine> storageEngineSupplier, TokenType type,
                           ReplicatedTokenCreator tokenCreator, PageCacheTracer pageCacheTracer,
                           LogEntryWriterFactory logEntryWriterFactory, ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
    {
        super( tokenRegistry );
        this.replicator = replicator;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.type = type;
        this.storageEngineSupplier = storageEngineSupplier;
        this.tokenCreator = tokenCreator;
        this.namedDatabaseId = namedDatabaseId;
        this.pageCacheTracer = pageCacheTracer;
        this.logEntryWriterFactory = logEntryWriterFactory;
        this.readOnlyDatabaseChecker = readOnlyDatabaseChecker;
    }

    @Override
    public void getOrCreateIds( String[] names, int[] ids ) throws KernelException
    {
        for ( int i = 0; i < names.length; i++ )
        {
            ids[i] = innerGetOrCreateId( names[i], false );
        }
    }

    @Override
    public void getOrCreateInternalIds( String[] names, int[] ids ) throws KernelException
    {
        for ( int i = 0; i < names.length; i++ )
        {
            ids[i] = innerGetOrCreateId( names[i], true );
        }
    }

    @Override
    protected int createToken( String tokenName, boolean internal )
    {
        validateNotReadOnly( readOnlyDatabaseChecker );

        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( namedDatabaseId.databaseId(), type,
                                                                          tokenName, createCommands( tokenName, internal ) );
        ReplicationResult replicationResult = replicator.replicate( tokenRequest );

        switch ( replicationResult.outcome() )
        {
        case NOT_REPLICATED:
            // The caller can safely retry this action because we know it was not replicated
            throw new TransientTransactionFailureException( "Could not replicate token for " + namedDatabaseId, replicationResult.failure() );
        case MAYBE_REPLICATED:
            // The caller can safely retry this action because it is idempotent.
            throw new TransientTransactionFailureException( "Could not replicate token for " + namedDatabaseId, replicationResult.failure() );
        case APPLIED:
            break;
        default:
            throw new IllegalArgumentException( "Unknown replication result outcome: " + replicationResult.outcome().toString() );
        }

        try
        {
            return replicationResult.stateMachineResult().consume();
        }
        catch ( TransientTransactionFailureException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            // the ReplicatedTokenStateMachine does not produce exceptions as a result
            throw new IllegalStateException( e );
        }
    }

    private void validateNotReadOnly( ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
    {
        if ( readOnlyDatabaseChecker.test( namedDatabaseId.name() ) )
        {
            throw new RuntimeException( new ReadOnlyDbException( namedDatabaseId.name() ) );
        }
    }

    private byte[] createCommands( String tokenName, boolean internal )
    {
        StorageEngine storageEngine = storageEngineSupplier.get();
        List<StorageCommand> commands = new ArrayList<>();
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        TransactionState txState = new TxState( OnHeapCollectionsFactory.INSTANCE, memoryTracker );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( REPLICATED_TOKEN_HOLDER_CREATOR_TAG ) )
        {
            int tokenId = Math.toIntExact( idGeneratorFactory.get( tokenIdType ).nextId( cursorTracer ) );
            tokenCreator.createToken( txState, tokenName, internal, tokenId );
            try ( StorageReader reader = storageEngine.newReader();
                    CommandCreationContext creationContext = storageEngine.newCommandCreationContext( cursorTracer,memoryTracker ) )
            {
                storageEngine.createCommands( commands, txState, reader, creationContext, ResourceLocker.PREVENT, LockTracer.NONE, Long.MAX_VALUE,
                        NO_DECORATION, cursorTracer, memoryTracker );
            }
            catch ( KernelException e )
            {
                throw new RuntimeException( "Unable to create token '" + tokenName + "' for " + namedDatabaseId, e );
            }
        }

        return StorageCommandMarshal.commandsToBytes( commands, logEntryWriterFactory );
    }
}
