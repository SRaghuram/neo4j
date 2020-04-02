/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.routing;

import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.localdb.FabricDatabaseManager;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedure;
import org.neo4j.values.virtual.MapValue;

public class FabricSingleInstanceGetRoutingTableProcedure extends SingleInstanceGetRoutingTableProcedure
{

    private final FabricConfig fabricConfig;
    private final FabricDatabaseManager fabricDatabaseManager;

    public FabricSingleInstanceGetRoutingTableProcedure( List<String> namespace, DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            Config config, FabricDatabaseManager fabricDatabaseManager, FabricConfig fabricConfig, LogProvider logProvider )
    {
        super( namespace, databaseManager, portRegister, config, logProvider );
        this.fabricDatabaseManager = fabricDatabaseManager;
        this.fabricConfig = fabricConfig;
    }

    @Override
    protected RoutingResult invoke( NamedDatabaseId namedDatabaseId, MapValue routingContext ) throws ProcedureException
    {
        if ( fabricDatabaseManager.hasFabricRoutingTable( namedDatabaseId.name() ) )
        {
            var fabricServers = fabricConfig.getFabricServers();
            return new RoutingResult( fabricServers, fabricServers, fabricServers, fabricConfig.getRoutingTtl().toSeconds() );
        }

        return super.invoke( namedDatabaseId, routingContext );
    }
}
