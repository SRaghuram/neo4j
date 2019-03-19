/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.replication.ReplicationFailureException;
import com.neo4j.causalclustering.core.replication.Replicator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.AbstractTokenHolderBase;
import org.neo4j.token.TokenRegistry;

import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

public class ReplicatedTokenHolder extends AbstractTokenHolderBase
{
    private final Replicator replicator;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType tokenIdType;
    private final TokenType type;
    private final Supplier<StorageEngine> storageEngineSupplier;
    private final ReplicatedTokenCreator tokenCreator;
    private final String databaseName;

    ReplicatedTokenHolder( String databaseName, TokenRegistry tokenRegistry, Replicator replicator,
                           IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
                           Supplier<StorageEngine> storageEngineSupplier, TokenType type,
                           ReplicatedTokenCreator tokenCreator )
    {
        super( tokenRegistry );
        this.replicator = replicator;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.type = type;
        this.storageEngineSupplier = storageEngineSupplier;
        this.tokenCreator = tokenCreator;
        this.databaseName = databaseName;
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
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( databaseName, type, tokenName, createCommands( tokenName, internal ) );
        try
        {
            return (int) replicator.replicate( tokenRequest ).consume();
        }
        catch ( ReplicationFailureException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Could not create token", e );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( e );
        }
    }

    private byte[] createCommands( String tokenName, boolean internal )
    {
        StorageEngine storageEngine = storageEngineSupplier.get();
        Collection<StorageCommand> commands = new ArrayList<>();
        TransactionState txState = new TxState();
        int tokenId = Math.toIntExact( idGeneratorFactory.get( tokenIdType ).nextId() );
        tokenCreator.createToken( txState, tokenName, internal, tokenId );
        try ( StorageReader reader = storageEngine.newReader();
              CommandCreationContext creationContext = storageEngine.newCommandCreationContext() )
        {
            storageEngine.createCommands( commands, txState, reader, creationContext, ResourceLocker.NONE, Long.MAX_VALUE, NO_DECORATION );
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( "Unable to create token '" + tokenName + "'", e );
        }

        return StorageCommandMarshal.commandsToBytes( commands );
    }
}
