/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;

import static java.util.function.Function.identity;

public class StandaloneClusterOverviewProcedure extends ClusterOverviewProcedure
{
    private final ServerId serverId;
    private final Config config;
    private final DatabaseManager<?> databaseManager;

    public StandaloneClusterOverviewProcedure( ServerId serverId, Config config, DatabaseManager<?> databaseManager )
    {
        super();
        this.serverId = serverId;
        this.config = config;
        this.databaseManager = databaseManager;
    }

    @Override
    protected List<ResultRow> produceResultRows()
    {
        var connectorAddresses = ConnectorAddresses.fromConfig( config );
        var groups = Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
        var databases = databaseManager.registeredDatabases().keySet().stream().collect( Collectors.toMap( identity(), ignored -> RoleInfo.LEADER ) );
        var resultRow = new ResultRow( serverId.uuid(), connectorAddresses, databases, groups );
        return List.of( resultRow );
    }
}
