/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

class Difference
{
    private MemberId memberId;
    private CatchupServerAddress server;

    private Difference( MemberId memberId, CatchupServerAddress server )
    {
        this.memberId = memberId;
        this.server = server;
    }

    static <T extends DiscoveryServerInfo> Difference asDifference( Topology<T> topology, MemberId memberId )
    {
        return new Difference( memberId, topology.find( memberId ).orElse( null ) );
    }

    @Override
    public String toString()
    {
        return String.format( "{memberId=%s, info=%s}", memberId, server );
    }
}
