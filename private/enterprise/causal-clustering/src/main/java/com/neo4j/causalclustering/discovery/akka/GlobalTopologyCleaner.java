/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;

class GlobalTopologyCleaner
{
    private final MemberId localMemberId;

    GlobalTopologyCleaner( MemberId localMemberId )
    {
        this.localMemberId = localMemberId;
    }

    DatabaseCoreTopology removeRemoteDatabaseCoreTopologies( DatabaseId databaseId, DatabaseCoreTopology databaseCoreTopology )
    {
        return new DatabaseCoreTopology(
                databaseId,
                databaseCoreTopology.raftId(),
                databaseCoreTopology.servers()
                                    .containsKey( localMemberId ) ? Map.of( localMemberId, databaseCoreTopology.servers().get( localMemberId ) )
                                                                  : emptyMap()
        );
    }

    ReplicatedDatabaseState removeRemoteReplicatedDatabaseState( DatabaseId databaseId, ReplicatedDatabaseState coreStates )
    {
        return coreStates.stateFor( localMemberId )
                         .map( coreState -> ReplicatedDatabaseState.ofCores( databaseId, Map.of( localMemberId, coreState ) ) )
                         .orElse( ReplicatedDatabaseState.ofCores( databaseId, emptyMap() ) );
    }

    DatabaseReadReplicaTopology removeRemoteDatabaseReadReplicaTopologies( DatabaseId databaseId, DatabaseReadReplicaTopology readReplicaTopology )
    {
        return new DatabaseReadReplicaTopology(
                databaseId,
                readReplicaTopology.servers()
                                   .containsKey( localMemberId ) ? Map.of( localMemberId, readReplicaTopology.servers().get( localMemberId ) )
                                                                 : emptyMap()
        );
    }
}
