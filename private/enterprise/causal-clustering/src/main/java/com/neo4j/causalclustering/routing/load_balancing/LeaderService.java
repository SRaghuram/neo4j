/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;

public interface LeaderService
{
    Optional<MemberId> getLeaderId( String databaseName );

    Optional<AdvertisedSocketAddress> getLeaderBoltAddress( String databaseName );
}
