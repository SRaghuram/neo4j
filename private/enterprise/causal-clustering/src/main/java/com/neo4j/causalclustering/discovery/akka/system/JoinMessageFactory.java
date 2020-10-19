/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.neo4j.causalclustering.discovery.RemoteMembersResolver;

public class JoinMessageFactory
{
    private final RemoteMembersResolver resolver;
    private final Set<Address> allSeenAddresses = new HashSet<>();

    public JoinMessageFactory( RemoteMembersResolver resolver )
    {
        this.resolver = resolver;
    }

    JoinMessage message()
    {
        JoinMessage message = JoinMessage.initial( allSeenAddresses );
        allSeenAddresses.clear();
        return message;
    }

    void addSeenAddresses( Collection<Address> addresses )
    {
        if ( resolver.useOverrides() )
        {
            allSeenAddresses.addAll( addresses );
        }
    }
}
