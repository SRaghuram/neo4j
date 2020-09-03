/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;

final class GlobalTopologyStateTestUtil
{
    private GlobalTopologyStateTestUtil()
    {
    }

    static void setupCoreTopologyState( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, ServerId... coreIds )
    {
        var databaseId = namedDatabaseId.databaseId();
        var coreMembers = new HashMap<ServerId,CoreServerInfo>();

        if ( coreIds != null )
        {
            for ( var i = 0; i < coreIds.length; i++ )
            {
                coreMembers.put( coreIds[i], addressesForCore( i + 1, false, Set.of( databaseId ) ) );
            }
        }

        var coreTopology = new DatabaseCoreTopology( databaseId, RaftGroupId.from( databaseId ), coreMembers );
        topologyState.onTopologyUpdate( coreTopology );
    }

    static void setupRaftMapping( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, Map<ServerId,RaftMemberId> mapping )
    {
        var databaseId = namedDatabaseId.databaseId();
        mapping.forEach(
                ( serverId, raftMemberId ) -> topologyState.onRaftMappingUpdate( ReplicatedRaftMapping.of( serverId, Map.of( databaseId, raftMemberId ) ) ) );
    }

    static void setupLeader( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, RaftMemberId leaderId )
    {
        var databaseId = namedDatabaseId.databaseId();
        topologyState.onDbLeaderUpdate( Map.of( databaseId, new LeaderInfo( leaderId, 42 ) ) );
    }

    static void setupReadReplicaTopologyState( GlobalTopologyState topologyState, NamedDatabaseId namedDatabaseId, ServerId... readReplicaIds )
    {
        var databaseId = namedDatabaseId.databaseId();
        var readReplicas = new HashMap<ServerId,ReadReplicaInfo>();

        if ( readReplicaIds != null )
        {
            for ( int i = 0; i < readReplicaIds.length; i++ )
            {
                readReplicas.put( readReplicaIds[i], addressesForReadReplica( i + 1, Set.of( databaseId ) ) );
            }
        }

        var readReplicaTopology = new DatabaseReadReplicaTopology( databaseId, readReplicas );
        topologyState.onTopologyUpdate( readReplicaTopology );
    }
}
