/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;

public class ReplicatedLeaseRequest implements CoreReplicatedContent, Lease
{
    private final RaftMemberId owner;
    private final int leaseId;
    private final DatabaseId databaseId;

    static final ReplicatedLeaseRequest INVALID_LEASE_REQUEST = new ReplicatedLeaseRequest( null, NO_LEASE, null );

    public ReplicatedLeaseRequest( ReplicatedLeaseState state, NamedDatabaseId namedDatabaseId )
    {
        this( state.owner(), state.leaseId(), namedDatabaseId.databaseId() );
    }

    public ReplicatedLeaseRequest( RaftMemberId owner, int leaseId, DatabaseId databaseId )
    {
        this.owner = owner;
        this.leaseId = leaseId;
        this.databaseId = databaseId;
    }

    @Override
    public int id()
    {
        return leaseId;
    }

    @Override
    public RaftMemberId owner()
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
        ReplicatedLeaseRequest that = (ReplicatedLeaseRequest) o;
        return leaseId == that.leaseId && Objects.equals( owner, that.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( owner, leaseId );
    }

    @Override
    public String toString()
    {
        return format( "ReplicatedLeaseRequest{owner=%s, leaseId=%d}", owner, leaseId );
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
