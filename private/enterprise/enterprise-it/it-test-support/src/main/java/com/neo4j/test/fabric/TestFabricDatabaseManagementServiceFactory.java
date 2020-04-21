/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.fabric;

import com.neo4j.fabric.bolt.BoltFabricDatabaseManagementService;

import java.util.function.Function;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManagementServiceImpl;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.TestDatabaseManagementServiceFactory;
import org.neo4j.time.SystemNanoClock;

public class TestFabricDatabaseManagementServiceFactory extends TestDatabaseManagementServiceFactory
{
    private final Config config;

    public TestFabricDatabaseManagementServiceFactory( DatabaseInfo databaseInfo,
                                                       Function<GlobalModule,AbstractEditionModule> editionFactory,
                                                       boolean impermanent,
                                                       FileSystemAbstraction fileSystem,
                                                       SystemNanoClock clock,
                                                       LogProvider internalLogProvider,
                                                       Config config )
    {
        super( databaseInfo, editionFactory, impermanent, fileSystem, clock, internalLogProvider );

        this.config = config;
    }

    @Override
    protected DatabaseManagementService createManagementService( GlobalModule globalModule, LifeSupport globalLife, Log internalLog,
                                                                 DatabaseManager<?> databaseManager )
    {
        return new DatabaseManagementServiceImpl( databaseManager, globalModule.getGlobalAvailabilityGuard(),
                                                  globalLife, globalModule.getDatabaseEventListeners(), globalModule.getTransactionEventListeners(),
                                                  internalLog )
        {
            @Override
            public GraphDatabaseService database( String name ) throws DatabaseNotFoundException
            {
                BoltFabricDatabaseManagementService fabricBoltDbms =
                        globalModule.getGlobalDependencies().resolveDependency( BoltFabricDatabaseManagementService.class );

                try
                {
                    var baseDb = databaseManager.getDatabaseContext( name )
                                                .orElseThrow( () -> new DatabaseNotFoundException( name ) ).databaseFacade();
                    BoltGraphDatabaseServiceSPI fabricBoltDb = fabricBoltDbms.database( name );
                    return new TestFabricGraphDatabaseService( baseDb, fabricBoltDb, config );
                }
                catch ( UnavailableException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
}