/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.replication.ReplicationFailureException;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.core.NonUniqueTokenException;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.lock.ResourceLocker;

import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.NO_DECORATION;

abstract class ReplicatedTokenHolder implements TokenHolder
{
    private final Replicator replicator;
    private final TokenRegistry tokenRegistry;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdType tokenIdType;
    private final TokenType type;
    private final Supplier<StorageEngine> storageEngineSupplier;

    // TODO: Clean up all the resolving, which now happens every time with special selection strategies.
    ReplicatedTokenHolder( TokenRegistry tokenRegistry, Replicator replicator,
                           IdGeneratorFactory idGeneratorFactory, IdType tokenIdType,
                           Supplier<StorageEngine> storageEngineSupplier, TokenType type )
    {
        this.replicator = replicator;
        this.tokenRegistry = tokenRegistry;
        this.idGeneratorFactory = idGeneratorFactory;
        this.tokenIdType = tokenIdType;
        this.type = type;
        this.storageEngineSupplier = storageEngineSupplier;
    }

    @Override
    public void setInitialTokens( List<NamedToken> tokens ) throws NonUniqueTokenException
    {
        tokenRegistry.setInitialTokens( tokens );
    }

    @Override
    public void addToken( NamedToken token ) throws NonUniqueTokenException
    {
        tokenRegistry.put( token );
    }

    @Override
    public int getOrCreateId( String tokenName )
    {
        Integer tokenId = tokenRegistry.getId( tokenName );
        if ( tokenId != null )
        {
            return tokenId;
        }

        return requestToken( tokenName );
    }

    @Override
    public void getOrCreateIds( String[] names, int[] ids )
    {
        // todo This could be optimised, but doing so requires a protocol change.
        for ( int i = 0; i < names.length; i++ )
        {
            ids[i] = getOrCreateId( names[i] );
        }
    }

    private int requestToken( String tokenName )
    {
        ReplicatedTokenRequest tokenRequest = new ReplicatedTokenRequest( type, tokenName, createCommands( tokenName ) );
        try
        {
            Future<Object> future = replicator.replicate( tokenRequest, true );
            return (int) future.get();
        }
        catch ( ReplicationFailureException | InterruptedException e )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( "Could not create token", e );
        }
        catch ( ExecutionException e )
        {
            throw new IllegalStateException( e );
        }
    }

    private byte[] createCommands( String tokenName )
    {
        StorageEngine storageEngine = storageEngineSupplier.get();
        Collection<StorageCommand> commands = new ArrayList<>();
        TransactionState txState = new TxState();
        int tokenId = Math.toIntExact( idGeneratorFactory.get( tokenIdType ).nextId() );
        createToken( txState, tokenName, tokenId );
        try ( StorageReader statement = storageEngine.newReader() )
        {
            storageEngine.createCommands( commands, txState, statement, ResourceLocker.NONE, Long.MAX_VALUE, NO_DECORATION );
        }
        catch ( CreateConstraintFailureException | TransactionFailureException | ConstraintValidationException e )
        {
            throw new RuntimeException( "Unable to create token '" + tokenName + "'", e );
        }

        return ReplicatedTokenRequestSerializer.commandBytes( commands );
    }

    protected abstract void createToken( TransactionState txState, String tokenName, int tokenId );

    @Override
    public NamedToken getTokenById( int id ) throws TokenNotFoundException
    {
        NamedToken result = getTokenByIdOrNull( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    @Override
    public NamedToken getTokenByIdOrNull( int id )
    {
        return tokenRegistry.getToken( id );
    }

    @Override
    public int getIdByName( String name )
    {
        Integer id = tokenRegistry.getId( name );
        if ( id == null )
        {
            return NO_TOKEN;
        }
        return id;
    }

    @Override
    public boolean getIdsByNames( String[] names, int[] ids )
    {
        boolean hasUnresolvedTokens = false;
        for ( int i = 0; i < names.length; i++ )
        {
            Integer id = tokenRegistry.getId( names[i] );
            if ( id == null )
            {
                hasUnresolvedTokens = true;
            }
            else
            {
                ids[i] = id;
            }
        }
        return hasUnresolvedTokens;
    }

    @Override
    public Iterable<NamedToken> getAllTokens()
    {
        return tokenRegistry.allTokens();
    }

    @Override
    public int size()
    {
        return tokenRegistry.size();
    }
}
