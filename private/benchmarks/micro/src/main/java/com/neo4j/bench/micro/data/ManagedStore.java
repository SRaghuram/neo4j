/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.micro.data.Stores.StoreAndConfig;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.common.util.BenchmarkUtil.bytesToString;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class ManagedStore
{
    private static final Logger LOG = LoggerFactory.getLogger( ManagedStore.class );

    private final DataGeneratorConfig dataGeneratorConfig;
    private final StoreAndConfig storeAndConfig;
    protected GraphDatabaseService db;
    private static DatabaseManagementService managementService;

    public ManagedStore( DataGeneratorConfig dataGeneratorConfig, StoreAndConfig storeAndConfig )
    {
        this.dataGeneratorConfig = dataGeneratorConfig;
        this.storeAndConfig = storeAndConfig;
    }

    public GraphDatabaseService startDb()
    {
        if ( isDatabaseRunning() )
        {
            throw new RuntimeException( "Can not start an already running database" );
        }
        return db = newDb( storeAndConfig.store(), storeAndConfig.config() );
    }

    public static GraphDatabaseService newDb( Store store )
    {
        return newDb( store, null );
    }

    public static GraphDatabaseService newDb( Store store, Path config )
    {
        DatabaseManagementServiceBuilder builder = new EnterpriseDatabaseManagementServiceBuilder( store.topLevelDirectory() );
        if ( null != config )
        {
            builder = builder.loadPropertiesFromFile( config.toFile().getAbsolutePath() );
        }
        managementService = builder.build();

        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    public static DatabaseManagementService getManagementService()
    {
        return managementService;
    }

    public void tearDownDb() throws IOException
    {
        if ( isDatabaseRunning() )
        {
            managementService.shutdown();
        }
        if ( !dataGeneratorConfig.isReusable() )
        {
            LOG.debug( format( "Deleting store [%s] at: %s",
                               bytesToString( storeAndConfig.store().bytes() ), storeAndConfig.store().topLevelDirectory() ) );
            FileUtils.deleteDirectory( storeAndConfig.store().topLevelDirectory() );
        }
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public GraphDatabaseService systemDb()
    {
        return managementService.database( SYSTEM_DATABASE_NAME );
    }

    public Store store()
    {
        return storeAndConfig.store();
    }

    private boolean isDatabaseRunning()
    {
        return db != null && db.isAvailable( SECONDS.toMillis( 1 ) );
    }
}
