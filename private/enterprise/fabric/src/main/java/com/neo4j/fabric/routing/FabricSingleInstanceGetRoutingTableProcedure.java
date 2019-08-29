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
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedure;
import org.neo4j.values.virtual.MapValue;

public class FabricSingleInstanceGetRoutingTableProcedure extends SingleInstanceGetRoutingTableProcedure
{

    private final FabricDatabaseManager fabricDatabaseManager;

    public FabricSingleInstanceGetRoutingTableProcedure( List<String> namespace, DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            Config config, FabricDatabaseManager fabricDatabaseManager, LogProvider logProvider )
    {
        super( namespace, databaseManager, portRegister, config, logProvider );
        this.fabricDatabaseManager = fabricDatabaseManager;
    }

    @Override
    protected RoutingResult invoke( DatabaseId databaseId, MapValue routingContext )
    {
        if ( fabricDatabaseManager.isFabricDatabase( databaseId.name() ) )
        {
            // TODO: this is where Fabric logic gets plugged in
            throw new IllegalStateException( "Fabric is not here yet" );
        }

        return super.invoke( databaseId, routingContext );
    }
}
