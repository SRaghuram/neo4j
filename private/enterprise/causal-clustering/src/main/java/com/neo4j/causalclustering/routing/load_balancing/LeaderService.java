/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import java.util.Optional;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

public interface LeaderService
{
    Optional<ServerId> getLeaderId( NamedDatabaseId namedDatabaseId );

    Optional<SocketAddress> getLeaderBoltAddress( NamedDatabaseId namedDatabaseId );
}
