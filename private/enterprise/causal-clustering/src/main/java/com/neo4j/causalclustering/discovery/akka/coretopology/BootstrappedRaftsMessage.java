/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.Map;
import java.util.Objects;

/**
 * Sent from discovery service to this Neo4J instance
 */
public class BootstrappedRaftsMessage
{
    public static final BootstrappedRaftsMessage EMPTY = new BootstrappedRaftsMessage( Map.of() );
    private final Map<RaftGroupId,RaftMemberId> bootstrappedRafts;

    public BootstrappedRaftsMessage( Map<RaftGroupId,RaftMemberId> bootstrappedRafts )
    {
        this.bootstrappedRafts = Map.copyOf( bootstrappedRafts );
    }

    public Map<RaftGroupId,RaftMemberId> bootstrappedRafts()
    {
        return bootstrappedRafts;
    }

    @Override
    public String toString()
    {
        return "BootstrappedRaftsMessage{" + "bootstrappedRafts=" + bootstrappedRafts + '}';
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
        BootstrappedRaftsMessage that = (BootstrappedRaftsMessage) o;
        return Objects.equals( bootstrappedRafts, that.bootstrappedRafts );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( bootstrappedRafts );
    }
}
