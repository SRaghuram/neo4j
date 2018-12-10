/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.UUID;

import static java.lang.String.format;

public class GlobalSession
{
    private final UUID sessionId;
    private final MemberId owner;

    public GlobalSession( UUID sessionId, MemberId owner )
    {
        this.sessionId = sessionId;
        this.owner = owner;
    }

    public UUID sessionId()
    {
        return sessionId;
    }

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

        GlobalSession that = (GlobalSession) o;

        if ( !sessionId.equals( that.sessionId ) )
        {
            return false;
        }
        return owner.equals( that.owner );
    }

    @Override
    public int hashCode()
    {
        int result = sessionId.hashCode();
        result = 31 * result + owner.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return format( "GlobalSession{sessionId=%s, owner=%s}", sessionId, owner );
    }
}
