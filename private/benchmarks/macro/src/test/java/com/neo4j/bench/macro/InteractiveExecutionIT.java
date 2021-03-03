/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.execution.Options;
import com.neo4j.bench.macro.execution.OptionsBuilder;
import com.neo4j.bench.macro.execution.process.MeasurementOptions;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.options.Edition;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.createTempDirectory;

@TestDirectoryExtension
class InteractiveExecutionIT
{
    private static final String WORKLOAD = "zero";

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void executeWorkloadInteractivelyWithEmbedded() throws Exception
    {
        executeWorkloadInteractively( WORKLOAD, Deployment.embedded() );
    }

    private void executeWorkloadInteractively( String workloadName, Deployment deployment ) throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.absolutePath() ) )
        {
            Workload workload = Workload.fromName( workloadName, resources, deployment );
            Path neo4jConfigFile = temporaryFolder.createFile( "neo4j.conf" );
            Store store = StoreTestUtil.createEmptyStoreFor( workload,
                                                             temporaryFolder.directory( "store" ), // store
                                                             neo4jConfigFile );
            Path dataset = store.topLevelDirectory();
            MeasurementOptions measurementOptions = new MeasurementOptions( 1,
                                                                            1,
                                                                            Duration.ofSeconds( 0 ),
                                                                            Duration.ofSeconds( 10 ) );
            Jvm jvm = Jvm.defaultJvmOrFail();
            Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( deployment, Edition.ENTERPRISE, measurementOptions, jvm, dataset );

            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfigFile );
            OptionsBuilder optionsBuilder = new OptionsBuilder()
                    .withNeo4jConfig( neo4jConfigFile )
                    .withForks( 0 )
                    .withUnit( TimeUnit.MICROSECONDS )
                    .withJvm( jvm )
                    .withNeo4jDeployment( neo4jDeployment );

            for ( Query query : workload.queries() )
            {
                Path outputDir = createTempDirectory( temporaryFolder.absolutePath(), "output" );
                Options options = optionsBuilder
                        .withOutputDir( outputDir )
                        .withStoreDir( dataset )
                        .withQuery( query )
                        .build();
                Main.runInteractive( options );
            }
        }
    }
}
