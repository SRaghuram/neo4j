/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.roles.follower;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * This presents a read only view over the map of members to their states. Instances that are not present
 * in the map will have the default FollowerState returned.
 * @param <MEMBER> The type of member id
 */
public class FollowerStates<MEMBER>
{
    private final Map<MEMBER, FollowerState> states;

    public FollowerStates( FollowerStates<MEMBER> original, MEMBER updatedMember, FollowerState updatedState )
    {
        this.states = new HashMap<>( original.states );
        states.put( updatedMember, updatedState );
    }

    public FollowerStates()
    {
        states = new HashMap<>();
    }

    public FollowerState get( MEMBER member )
    {
        FollowerState result = states.get( member );
        if ( result == null )
        {
            result = new FollowerState();
        }
        return result;
    }

    @Override
    public String toString()
    {
        return format( "FollowerStates%s", states );
    }

    public int size()
    {
        return states.size();
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

        FollowerStates that = (FollowerStates) o;

        return !(states != null ? !states.equals( that.states ) : that.states != null);
    }

    @Override
    public int hashCode()
    {
        return states != null ? states.hashCode() : 0;
    }

    public FollowerStates<MEMBER> onSuccessResponse( MEMBER member, long newMatchIndex )
    {
        return new FollowerStates<>( this, member, get( member ).onSuccessResponse( newMatchIndex ) );
    }
}
