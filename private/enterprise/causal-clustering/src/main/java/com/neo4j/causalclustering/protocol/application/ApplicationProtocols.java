/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.application;

import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.configuration.ApplicationProtocolVersion;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.function.Predicates;

public enum ApplicationProtocols implements ApplicationProtocol
{
    // support for raft 1.0 was removed in neo4j 4.0
    RAFT_2_0( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 2, 0 ) ),
    RAFT_3_0( ApplicationProtocolCategory.RAFT, new ApplicationProtocolVersion( 3, 0 ) ),

    // support for catchup 1.0 and 2.0 was removed in neo4j 4.0
    CATCHUP_3_0( ApplicationProtocolCategory.CATCHUP, new ApplicationProtocolVersion( 3, 0 ) );

    private final ApplicationProtocolVersion version;
    private final ApplicationProtocolCategory identifier;

    ApplicationProtocols( ApplicationProtocolCategory identifier, ApplicationProtocolVersion version )
    {
        this.identifier = identifier;
        this.version = version;
    }

    @Override
    public String category()
    {
        return identifier.canonicalName();
    }

    @Override
    public ApplicationProtocolVersion implementation()
    {
        return version;
    }

    public static Optional<ApplicationProtocol> find( ApplicationProtocolCategory category, ApplicationProtocolVersion version )
    {
        return Protocol.find( ApplicationProtocols.values(), category, version, Function.identity() );
    }

    public static List<ApplicationProtocol> withCategory( ApplicationProtocolCategory category )
    {
        return Protocol.filterCategory( ApplicationProtocols.values(), category, Predicates.alwaysTrue() );
    }
}
