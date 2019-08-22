/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.routing;

import com.neo4j.fabric.localdb.FabricDatabaseManager;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;

public class FabricRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{

    private final DatabaseManager<?> databaseManager;
    private final ConnectorPortRegister portRegister;
    private final Config config;
    private final FabricDatabaseManager fabricDatabaseManager;

    public FabricRoutingProcedureInstaller( DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            Config config, FabricDatabaseManager fabricDatabaseManager )
    {
        this.databaseManager = databaseManager;
        this.portRegister = portRegister;
        this.config = config;
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        return new FabricSingleInstanceGetRoutingTableProcedure( namespace, databaseManager, portRegister, config, fabricDatabaseManager );
    }
}
