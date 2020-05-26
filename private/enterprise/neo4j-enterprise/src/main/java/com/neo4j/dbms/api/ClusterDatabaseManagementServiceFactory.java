/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.api;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;

public class ClusterDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory
{
    public ClusterDatabaseManagementServiceFactory( DbmsInfo dbmsInfo, Function<GlobalModule,AbstractEditionModule> editionFactory )
    {
        super( dbmsInfo, editionFactory );
    }

    @Override
    protected ClusterDatabaseManagementService createManagementService( GlobalModule globalModule, LifeSupport globalLife, Log internalLog,
            DatabaseManager<?> databaseManager )
    {
        DatabaseManagementService delegate = super.createManagementService( globalModule, globalLife, internalLog, databaseManager );
        return new ClusterDatabaseManagementService( delegate );
    }

    @Override
    public ClusterDatabaseManagementService build( Config config, ExternalDependencies dependencies )
    {
        DatabaseManagementService dbms = super.build( config, dependencies );
        return new ClusterDatabaseManagementService( dbms );
    }
}
