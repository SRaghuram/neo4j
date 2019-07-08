/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.OptionsBuilder;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.common.util.TestSupport;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
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

    private void executeWorkloadInteractively( String workloadName, Neo4jDeployment deployment ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.newFolder().toPath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, deployment.mode() );
            Store store = createEmptyStoreFor( workload );

            Path neo4jConfigFile = temporaryFolder.newFile().toPath();
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );
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
