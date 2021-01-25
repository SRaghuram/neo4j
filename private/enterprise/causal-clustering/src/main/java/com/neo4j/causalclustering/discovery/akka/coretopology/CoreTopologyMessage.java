/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.Address;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;

import java.util.Collection;
import java.util.Objects;

public class CoreTopologyMessage
{
    private final DatabaseCoreTopology coreTopology;
    private final Collection<Address> akkaMembers;

    public CoreTopologyMessage( DatabaseCoreTopology coreTopology, Collection<Address> akkaMembers )
    {
        this.coreTopology = coreTopology;
        this.akkaMembers = akkaMembers;
    }

    public DatabaseCoreTopology coreTopology()
    {
        return coreTopology;
    }

    public Collection<Address> akkaMembers()
    {
        return akkaMembers;
    }

    @Override
    public String toString()
    {
        return "CoreTopologyMessage{" + "coreTopology=" + coreTopology + ", akkaMembers=" + akkaMembers + '}';
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
        CoreTopologyMessage that = (CoreTopologyMessage) o;
        return Objects.equals( coreTopology, that.coreTopology ) && Objects.equals( akkaMembers, that.akkaMembers );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( coreTopology, akkaMembers );
    }
}
