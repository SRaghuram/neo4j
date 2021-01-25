/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedTokenRequest implements CoreReplicatedContent
{
    private final TokenType type;
    private final String tokenName;
    private final byte[] commandBytes;
    private final DatabaseId databaseId;

    public ReplicatedTokenRequest( DatabaseId databaseId, TokenType type, String tokenName, byte[] commandBytes )
    {
        this.type = type;
        this.tokenName = tokenName;
        this.commandBytes = commandBytes;
        this.databaseId = databaseId;
    }

    public TokenType type()
    {
        return type;
    }

    String tokenName()
    {
        return tokenName;
    }

    byte[] commandBytes()
    {
        return commandBytes;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ReplicatedTokenRequest that = (ReplicatedTokenRequest) o;

        if ( type != that.type )
        {
            return false;
        }
        if ( !tokenName.equals( that.tokenName ) )
        {
            return false;
        }
        return Arrays.equals( commandBytes, that.commandBytes );

    }

    @Override
    public int hashCode()
    {
        int result = type.hashCode();
        result = 31 * result + tokenName.hashCode();
        result = 31 * result + Arrays.hashCode( commandBytes );
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "ReplicatedTokenRequest{type='%s', name='%s'}",
                type, tokenName );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<StateMachineResult> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }
}
