/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import java.util.List;
import java.util.Set;

public class ApplicationProtocolValidator
{
    public static void checkInstallersExhaustive( Set<ApplicationProtocol> keySet, ApplicationProtocolCategory category )
    {
        final var raftProtocolVersions = List.of( ApplicationProtocols.values() ).stream().filter( v -> v.category().equals( category.name() ) ).count();
        if ( raftProtocolVersions > keySet.size() )
        {
            throw new IllegalStateException( "There is unregistered client installer or server installer" );
        }
    }
}
