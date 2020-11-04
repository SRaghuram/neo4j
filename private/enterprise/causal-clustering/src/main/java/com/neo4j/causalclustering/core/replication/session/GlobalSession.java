/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.UUID;

import org.neo4j.function.Suppliers.Lazy;

import static java.lang.String.format;
import static org.neo4j.function.Suppliers.lazySingleton;

public class GlobalSession
{
    private final UUID sessionId;
    private final Lazy<RaftMemberId> owner;

    public GlobalSession( UUID sessionId, RaftMemberId owner )
    {
        this( sessionId, lazySingleton( () -> owner ) );
    }

    public GlobalSession( UUID sessionId, Lazy<RaftMemberId> owner )
    {
        this.sessionId = sessionId;
        this.owner = owner;
    }

    public UUID sessionId()
    {
        return sessionId;
    }

    public RaftMemberId owner()
    {
        return owner.get();
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

        GlobalSession that = (GlobalSession) o;

        if ( !sessionId.equals( that.sessionId ) )
        {
            return false;
        }
        return owner.get().equals( that.owner.get() );
    }

    @Override
    public int hashCode()
    {
        int result = sessionId.hashCode();
        result = 31 * result + owner.get().hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "GlobalSession{sessionId=%s, owner=%s}", sessionId, owner );
    }
}
