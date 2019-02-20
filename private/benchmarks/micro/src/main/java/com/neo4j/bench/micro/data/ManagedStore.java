/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.FullBenchmarkName;
import com.neo4j.bench.micro.data.Stores.StoreAndConfig;
import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.io.fs.FileUtils;

import static com.neo4j.bench.client.util.BenchmarkUtil.bytes;
import static com.neo4j.bench.client.util.BenchmarkUtil.bytesToString;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ManagedStore
{
    private final Stores stores;
    private DataGeneratorConfig dataGeneratorConfig;
    private StoreAndConfig storeAndConfig;
    protected GraphDatabaseService db;

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

    public static GraphDatabaseService newDb( Path dbPath )
    {
        return newDb( dbPath, null );
    }

    public static GraphDatabaseService newDb( Path dbPath, Path config )
    {
        GraphDatabaseBuilder builder = new CommercialGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbPath.toFile() );
        if ( null != config )
        {
            builder = builder.loadPropertiesFromFile( config.toFile().getAbsolutePath() );
        }
        return builder.newGraphDatabase();
    }

    public void tearDownDb() throws IOException
    {
        if ( isDatabaseRunning() )
        {
            db.shutdown();
        }
        if ( !dataGeneratorConfig.isReusable() )
        {
            System.out.println( format( "Deleting store [%s] at: %s",
                                        bytesToString( bytes( storeAndConfig.topLevelDir() ) ), storeAndConfig.topLevelDir() ) );
            FileUtils.deleteRecursively( storeAndConfig.topLevelDir().toFile() );
        }
    }

    public GraphDatabaseService db()
    {
        return db;
    }

    public Path store()
    {
        return storeAndConfig.store();
    }

    private boolean isDatabaseRunning()
    {
        return db != null && db.isAvailable( SECONDS.toMillis( 1 ) );
    }
}
