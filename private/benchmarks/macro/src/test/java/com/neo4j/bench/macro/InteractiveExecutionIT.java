/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.util.Resources;
import com.neo4j.bench.client.util.TestSupport;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.OptionsBuilder;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class InteractiveExecutionIT
{
    private static final String WORKLOAD = "zero";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void executeWorkloadInteractivelyWithEmbedded() throws Exception
    {
        executeWorkloadInteractively( WORKLOAD, Neo4jDeployment.embedded() );
    }

    @Test
    public void executeWorkloadInteractivelyWithServer() throws Exception
    {
        String neo4jDirString = System.getenv( "NEO4J_DIR" );
        Path neo4jDir = Paths.get( Objects.requireNonNull( neo4jDirString ) );
        executeWorkloadInteractively( WORKLOAD, Neo4jDeployment.server( neo4jDir ) );
    }

    private void executeWorkloadInteractively( String workloadName, Neo4jDeployment deployment ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, deployment.mode() );
            Store store = createEmptyStoreFor( workload );

            Path neo4jConfigFile = temporaryFolder.newFile().toPath();
            Neo4jConfig.withDefaults().writeToFile( neo4jConfigFile );
            OptionsBuilder optionsBuilder = new OptionsBuilder()
                    .withNeo4jConfig( neo4jConfigFile )
                    .withForks( 0 )
                    .withWarmupCount( 1 )
                    .withMeasurementCount( 1 )
                    .withMaxDuration( Duration.ofSeconds( 10 ) )
                    .withUnit( TimeUnit.MICROSECONDS )
                    .withNeo4jDeployment( deployment );

            for ( Query query : workload.queries() )
            {
                Path outputDir = temporaryFolder.newFolder().toPath();
                Options options = optionsBuilder
                        .withOutputDir( outputDir )
                        .withStoreDir( store.topLevelDirectory() )
                        .withQuery( query )
                        .build();
                Main.runInteractive( options );
            }
        }
    }

    // Create empty store with valid schema, as expected by workload
    private Store createEmptyStoreFor( Workload workload ) throws IOException
    {
        Schema schema = workload.expectedSchema();
        Store store = TestSupport.createEmptyStore( temporaryFolder.newFolder().toPath() );
        Path neo4jConfigFile = temporaryFolder.newFile().toPath();
        EmbeddedDatabase.recreateSchema( store, Edition.ENTERPRISE, neo4jConfigFile, schema );
        return store;
    }
}
