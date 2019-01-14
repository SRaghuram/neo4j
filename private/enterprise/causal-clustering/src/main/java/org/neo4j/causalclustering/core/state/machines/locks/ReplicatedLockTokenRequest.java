/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import static java.lang.String.format;

public class ReplicatedLockTokenRequest implements CoreReplicatedContent, LockToken
{
    private final MemberId owner;
    private final int candidateId;

    static final ReplicatedLockTokenRequest INVALID_REPLICATED_LOCK_TOKEN_REQUEST =
            new ReplicatedLockTokenRequest( null, LockToken.INVALID_LOCK_TOKEN_ID );

    public ReplicatedLockTokenRequest( MemberId owner, int candidateId )
    {
        this.owner = owner;
        this.candidateId = candidateId;
    }

    @Override
    public int id()
    {
        return candidateId;
    }

    @Override
    public MemberId owner()
    {
        return owner;
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
        ReplicatedLockTokenRequest that = (ReplicatedLockTokenRequest) o;
        return candidateId == that.candidateId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( owner, candidateId );
    }

    @Override
    public String toString()
    {
        return format( "ReplicatedLockTokenRequest{owner=%s, candidateId=%d}", owner, candidateId );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public void handle( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }
}
