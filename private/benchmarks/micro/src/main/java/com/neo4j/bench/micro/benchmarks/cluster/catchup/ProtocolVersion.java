/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.configuration.ApplicationProtocolVersion;

public enum ProtocolVersion
{
    V3( "3.0" ),
    V4( "4.0" ),
    LATEST( "4.0" );

    private final ApplicationProtocolVersion version;

    ProtocolVersion( String version )
    {
        this.version = ApplicationProtocolVersion.parse( version );
    }

    public ApplicationProtocolVersion version()
    {
        return version;
    }
}
