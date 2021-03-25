/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;

import java.util.ArrayList;
import java.util.function.Function;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TopologyBasedClusterOverviewProcedure extends ClusterOverviewProcedure
{
    private final TopologyService topologyService;
    private final DatabaseIdRepository databaseIdRepository;

    public TopologyBasedClusterOverviewProcedure( TopologyService topologyService, DatabaseIdRepository databaseIdRepository )
    {
        super();
        this.topologyService = topologyService;
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    protected ArrayList<ResultRow> produceResultRows()
    {
        var resultRows = new ArrayList<ResultRow>();

        for ( var entry : topologyService.allCoreServers().entrySet() )
        {
            var row = buildResultRowForCore( entry.getKey(), entry.getValue() );
            resultRows.add( row );
        }

        for ( var entry : topologyService.allReadReplicas().entrySet() )
        {
            var row = buildResultRowForReadReplica( entry.getKey(), entry.getValue() );
            resultRows.add( row );
        }
        return resultRows;
    }

    private ResultRow buildResultRowForCore( ServerId serverId, CoreServerInfo coreInfo )
    {
        return buildResultRow( serverId, coreInfo, databaseId -> topologyService.lookupRole( databaseId, serverId ) );
    }

    private ResultRow buildResultRowForReadReplica( ServerId serverId, ReadReplicaInfo readReplicaInfo )
    {
        return buildResultRow( serverId, readReplicaInfo, ignore -> RoleInfo.READ_REPLICA );
    }

    private ResultRow buildResultRow( ServerId serverId, DiscoveryServerInfo serverInfo, Function<NamedDatabaseId,RoleInfo> result )
    {
        var databases = serverInfo.startedDatabaseIds()
                .stream()
                .flatMap( databaseId -> databaseIdRepository.getById( databaseId ).stream() )
                .collect( toMap( identity(), result ) );

        return new ResultRow( serverId.uuid(), serverInfo.connectors(), databases, serverInfo.groups() );
    }
}
