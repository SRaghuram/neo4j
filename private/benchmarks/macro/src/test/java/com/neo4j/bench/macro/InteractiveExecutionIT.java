/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.OptionsBuilder;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.createTempFile;

@TestDirectoryExtension
class InteractiveExecutionIT
{
    private static final String WORKLOAD = "zero";

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void executeWorkloadInteractivelyWithEmbedded() throws Exception
    {
        executeWorkloadInteractively( WORKLOAD, Neo4jDeployment.from( Deployment.embedded() ) );
    }

    private void executeWorkloadInteractively( String workloadName, Neo4jDeployment neo4jDeployment ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, neo4jDeployment.deployment() );

            Path neo4jConfigFile = createTempFile( temporaryFolder.absolutePath(), "neo4j", ".conf" );
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );

            Store store = StoreTestUtil.createEmptyStoreFor( workload,
                                                             createTempDirectory( temporaryFolder.absolutePath(), "store" ), // store
                                                             createTempFile( temporaryFolder.absolutePath(), "neo4j", ".conf" ) ); // neo4j config
            OptionsBuilder optionsBuilder = new OptionsBuilder()
                    .withNeo4jConfig( neo4jConfigFile )
                    .withForks( 0 )
                    .withWarmupCount( 1 )
                    .withMeasurementCount( 1 )
                    .withMaxDuration( Duration.ofSeconds( 10 ) )
                    .withUnit( TimeUnit.MICROSECONDS )
                    .withNeo4jDeployment( neo4jDeployment );

            for ( Query query : workload.queries() )
            {
                Path outputDir = createTempDirectory( temporaryFolder.absolutePath(), "output" );
                Options options = optionsBuilder
                        .withOutputDir( outputDir )
                        .withStoreDir( store.topLevelDirectory() )
                        .withQuery( query )
                        .build();
                Main.runInteractive( options );
            }
        }
    }
}
