/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;

import java.util.Map;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptyMap;

class GlobalTopologyCleaner
{
    private final ServerId localServerId;

    GlobalTopologyCleaner( ServerId localServerId )
    {
        this.localServerId = localServerId;
    }

    DatabaseCoreTopology removeRemoteDatabaseCoreTopologies( DatabaseId databaseId, DatabaseCoreTopology databaseCoreTopology )
    {
        return new DatabaseCoreTopology(
                databaseId,
                databaseCoreTopology.raftGroupId(),
                databaseCoreTopology.servers()
                                    .containsKey( localServerId ) ? Map.of( localServerId, databaseCoreTopology.servers().get( localServerId ) )
                                                                  : emptyMap()
        );
    }

    ReplicatedDatabaseState removeRemoteReplicatedDatabaseState( DatabaseId databaseId, ReplicatedDatabaseState coreStates )
    {
        return coreStates.stateFor( localServerId )
                         .map( coreState -> ReplicatedDatabaseState.ofCores( databaseId, Map.of( localServerId, coreState ) ) )
                         .orElse( ReplicatedDatabaseState.ofCores( databaseId, emptyMap() ) );
    }

    DatabaseReadReplicaTopology removeRemoteDatabaseReadReplicaTopologies( DatabaseId databaseId, DatabaseReadReplicaTopology readReplicaTopology )
    {
        return new DatabaseReadReplicaTopology(
                databaseId,
                readReplicaTopology.servers()
                                   .containsKey( localServerId ) ? Map.of( localServerId, readReplicaTopology.servers().get( localServerId ) )
                                                                 : emptyMap()
        );
    }
}
