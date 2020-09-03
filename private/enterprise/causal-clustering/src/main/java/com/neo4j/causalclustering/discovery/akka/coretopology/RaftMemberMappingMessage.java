/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.ddata.LWWMap;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

public class RaftMemberMappingMessage
{
    public static final RaftMemberMappingMessage EMPTY = new RaftMemberMappingMessage( Collections.emptyMap() );

    private final Map<DatabaseServer,RaftMemberId> mapping;

    public RaftMemberMappingMessage( LWWMap<DatabaseServer,RaftMemberId> mapping )
    {
        this( mapping.getEntries() );
    }

    private RaftMemberMappingMessage( Map<DatabaseServer,RaftMemberId> mapping )
    {
        this.mapping = unmodifiableMap( mapping );
    }

    public Map<DatabaseServer,RaftMemberId> map()
    {
        return mapping;
    }

    @Override
    public String toString()
    {
        return "RaftMemberMappingMessage{" + "mapping=" + mapping + '}';
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
        RaftMemberMappingMessage that = (RaftMemberMappingMessage) o;
        return Objects.equals( mapping, that.mapping );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( mapping );
    }

}
