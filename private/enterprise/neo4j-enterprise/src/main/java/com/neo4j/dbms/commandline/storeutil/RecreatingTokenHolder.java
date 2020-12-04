/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.map.MutableMap;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.NonUniqueTokenException;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;

import static org.neo4j.internal.recordstorage.StoreTokens.createReadOnlyTokenHolder;

class RecreatingTokenHolder implements TokenHolder
{
    private final TokenHolder delegate;
    private final StoreCopyStats stats;
    private final MutableMap<String,List<NamedToken>> recreatedTokens;
    private final String tokenType;
    private int createdTokenCounter; // Guarded by 'this'.

    RecreatingTokenHolder( String tokenType, StoreCopyStats stats, MutableMap<String, List<NamedToken>> recreatedTokens )
    {
        this.tokenType = tokenType;
        this.delegate = createReadOnlyTokenHolder( tokenType );
        this.stats = stats;
        this.recreatedTokens = recreatedTokens;
    }

    @Override
    public synchronized void setInitialTokens( List<NamedToken> tokens ) throws NonUniqueTokenException
    {
        delegate.setInitialTokens( tokens );
    }

    @Override
    public void addToken( NamedToken token ) throws NonUniqueTokenException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getOrCreateId( String name )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getOrCreateIds( String[] names, int[] ids )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NamedToken getTokenById( int id )
    {
        try
        {
            return delegate.getTokenById( id );
        }
        catch ( TokenNotFoundException e )
        {
            stats.addCorruptToken( tokenType, id );
            String tokenName;
            do
            {
                createdTokenCounter++;
                tokenName = getTokenType() + "_" + createdTokenCounter;
            }
            while ( getIdByName( tokenName ) != TokenConstants.NO_TOKEN );
            NamedToken token = new NamedToken( tokenName, id );
            delegate.addToken( token );
            recreatedTokens.getIfAbsentPut( getTokenType(), ArrayList::new ).add( token );
            return token;
        }
    }

    @Override
    public synchronized int getIdByName( String name )
    {
        return delegate.getIdByName( name );
    }

    @Override
    public synchronized boolean getIdsByNames( String[] names, int[] ids )
    {
        return delegate.getIdsByNames( names, ids );
    }

    @Override
    public synchronized Iterable<NamedToken> getAllTokens()
    {
        return delegate.getAllTokens();
    }

    @Override
    public synchronized String getTokenType()
    {
        return delegate.getTokenType();
    }

    @Override
    public synchronized boolean hasToken( int id )
    {
        return delegate.hasToken( id );
    }

    @Override
    public synchronized int size()
    {
        return delegate.size();
    }

    @Override
    public void getOrCreateInternalIds( String[] names, int[] ids )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized NamedToken getInternalTokenById( int id ) throws TokenNotFoundException
    {
        return delegate.getInternalTokenById( id );
    }
}
