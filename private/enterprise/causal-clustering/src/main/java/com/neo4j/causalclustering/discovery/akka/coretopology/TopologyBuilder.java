/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.identity.ClusteringIdentityModuleImpl;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Map;
import javax.annotation.Nullable;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toMap;

public class TopologyBuilder
{
    DatabaseCoreTopology buildCoreTopology( DatabaseId databaseId, @Nullable RaftId raftId, ClusterViewMessage cluster, MetadataMessage memberData )
    {
        Map<MemberId,CoreServerInfo> coreMembers = cluster.availableMembers()
                .flatMap( memberData::getStream )
                .filter( member -> member.coreServerInfo().startedDatabaseIds().contains( databaseId ) )
                .collect( toMap( coreServerInfoForServerId -> ClusteringIdentityModuleImpl.fromServerId( coreServerInfoForServerId.serverId() ),
                        CoreServerInfoForServerId::coreServerInfo ) );

        return new DatabaseCoreTopology( databaseId, raftId, coreMembers );
    }
}
