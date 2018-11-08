/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.id;

import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;
import org.neo4j.kernel.impl.store.id.IdType;

import static java.lang.String.format;

/**
 * This type is handled by the ReplicatedIdAllocationStateMachine. */
public class ReplicatedIdAllocationRequest implements CoreReplicatedContent
{
    private final MemberId owner;
    private final IdType idType;
    private final long idRangeStart;
    private final int idRangeLength;
    private final String databaseName;

    public ReplicatedIdAllocationRequest( MemberId owner, IdType idType, long idRangeStart, int idRangeLength, String databaseName )
    {
        this.owner = owner;
        this.idType = idType;
        this.idRangeStart = idRangeStart;
        this.idRangeLength = idRangeLength;
        this.databaseName = databaseName;
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

        ReplicatedIdAllocationRequest that = (ReplicatedIdAllocationRequest) o;

        if ( idRangeStart != that.idRangeStart )
        {
            return false;
        }
        if ( idRangeLength != that.idRangeLength )
        {
            return false;
        }
        if ( !owner.equals( that.owner ) )
        {
            return false;
        }
        return idType == that.idType;
    }

    @Override
    public int hashCode()
    {
        int result = owner.hashCode();
        result = 31 * result + idType.hashCode();
        result = 31 * result + (int) (idRangeStart ^ (idRangeStart >>> 32));
        result = 31 * result + idRangeLength;
        return result;
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    public MemberId owner()
    {
        return owner;
    }

    public IdType idType()
    {
        return idType;
    }

    long idRangeStart()
    {
        return idRangeStart;
    }

    int idRangeLength()
    {
        return idRangeLength;
    }

    @Override
    public String toString()
    {
        return format( "ReplicatedIdAllocationRequest{owner=%s, idType=%s, idRangeStart=%d, idRangeLength=%d}", owner,
                idType, idRangeStart, idRangeLength );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
    }
}
