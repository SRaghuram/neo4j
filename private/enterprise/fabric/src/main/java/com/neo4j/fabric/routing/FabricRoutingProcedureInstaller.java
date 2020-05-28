/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.routing;

import com.neo4j.configuration.FabricEnterpriseConfig;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.FabricDatabaseManager;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;

public class FabricRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{

    private final DatabaseManager<?> databaseManager;
    private final ConnectorPortRegister portRegister;
    private final Config config;
    private final FabricDatabaseManager fabricDatabaseManager;
    private final FabricEnterpriseConfig fabricConfig;
    private final LogProvider logProvider;

    public FabricRoutingProcedureInstaller( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister, Config config,
            FabricDatabaseManager fabricDatabaseManager, FabricEnterpriseConfig fabricConfig,  LogProvider logProvider )
    {
        this.databaseManager = databaseManager;
        this.portRegister = portRegister;
        this.config = config;
        this.fabricDatabaseManager = fabricDatabaseManager;
        this.fabricConfig = fabricConfig;
        this.logProvider = logProvider;
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        return new FabricSingleInstanceGetRoutingTableProcedure( namespace, databaseManager, portRegister, config, fabricDatabaseManager,
                fabricConfig, logProvider );
    }
}
