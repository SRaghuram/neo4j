/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.micro.data.Stores.StoreAndConfig;
import com.neo4j.enterprise.edition.factory.EnterpriseDatabaseManagementServiceBuilder;

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

public class ManagedStore
{
    private final Stores stores;
    private DataGeneratorConfig dataGeneratorConfig;
    private StoreAndConfig storeAndConfig;
    protected GraphDatabaseService db;
    private static DatabaseManagementService managementService;

    public ManagedStore( Stores stores )
    {
        this.stores = stores;
    }

    public void prepareDb(
            BenchmarkGroup group,
            Benchmark benchmark,
            DataGeneratorConfig benchmarkConfig,
            Neo4jConfig baseNeo4jConfig,
            Augmenterizer augmenterizer,
            int threads )
    {
        FullBenchmarkName benchmarkName = FullBenchmarkName.from( group, benchmark );
        dataGeneratorConfig = DataGeneratorConfigBuilder
                .from( benchmarkConfig )
                .withNeo4jConfig( baseNeo4jConfig.mergeWith( benchmarkConfig.neo4jConfig() ) )
                .withRngSeed( DataGenerator.DEFAULT_RNG_SEED )
                .augmentedBy( augmenterizer.augmentKey( benchmarkName ) )
                .build();
        storeAndConfig = stores.prepareDb(
                dataGeneratorConfig,
                group,
                benchmark,
                augmenterizer,
                threads );
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
        DatabaseManagementServiceBuilder builder = new EnterpriseDatabaseManagementServiceBuilder( store.topLevelDirectory().toFile() );
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
            System.out.println( format( "Deleting store [%s] at: %s",
                                        bytesToString( storeAndConfig.store().bytes() ), storeAndConfig.store().topLevelDirectory() ) );
            FileUtils.deleteRecursively( storeAndConfig.store().topLevelDirectory().toFile() );
        }
    }

    public GraphDatabaseService db()
    {
        return db;
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
