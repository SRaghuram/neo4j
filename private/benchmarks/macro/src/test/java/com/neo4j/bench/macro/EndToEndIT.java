/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.test.BaseEndToEndIT;
import com.neo4j.common.util.TestSupport;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.io.FileMatchers.anExistingFile;

@TestDirectoryExtension
class EndToEndIT extends BaseEndToEndIT
{
    private static final Deployment DEPLOYMENT = Deployment.embedded();

    @Override
    protected String scriptName()
    {
        return "run-report-benchmarks.sh";
    }

    @Override
    protected Path getJar( Path baseDir )
    {
        return baseDir.resolve( "target/macro.jar" );
    }

    @Override
    protected List<String> processArgs( Resources resources,
                                        List<ProfilerType> profilers,
                                        String endpointUrl,
                                        Path baseDir,
                                        Jvm jvm,
                                        ResultStoreCredentials resultStoreCredentials )
    {
        // prepare neo4j config file
        Path neo4jConfig = temporaryFolder.createFile( "neo4j.config" ).toPath();
        Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

        // create empty store
        Path dbPath = temporaryFolder.directory( "db" ).toPath();
        TestSupport.createEmptyStore( dbPath, neo4jConfig );

        Path resultsPath = temporaryFolder.createFile( "results.json" ).toPath();

        Path workPath = temporaryFolder.directory( "work" ).toPath();

        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( DEPLOYMENT );

        return asList( "./" + scriptName(),
                       // workload
                       "zero",
                       // db
                       dbPath.toString(),
                       // warmup_count
                       "1",
                       // measurement_count
                       "1",
                       // db_edition
                       Edition.ENTERPRISE.name(),
                       // jvm
                       jvm.launchJava(),
                       // neo4j_config
                       neo4jConfig.toString(),
                       // work_dir
                       workPath.toString(),
                       // profilers
                       profilers.stream().map( ProfilerType::name ).collect( joining( "," ) ),
                       // forks
                       "1",
                       // results_path
                       resultsPath.toString(),
                       // time_unit
                       MILLISECONDS.name(),
                       resultStoreCredentials.boltUri(),
                       resultStoreCredentials.user(),
                       resultStoreCredentials.pass(),
                       "neo4j_commit",
                       // neo4j_version
                       "3.5.1",
                       "neo4j_branch",
                       "neo4j_branch_owner",
                       "tool_commit",
                       "tool_branch_owner",
                       "tool_branch",
                       // teamcity_build
                       "1",
                       // parent_teamcity_build
                       "0",
                       // execution_mode
                       ExecutionMode.EXECUTE.name(),
                       // jvm_args
                       "-Xmx1g",
                       // recreate_schema
                       "false",
                       // planner
                       Planner.DEFAULT.name(),
                       // runtime
                       Runtime.DEFAULT.name(),
                       "triggered_by",
                       // error_policy
                       ErrorPolicy.FAIL.name(),
                       // embedded OR server:<path>
                       neo4jDeployment.toString(),
                       "Q1",
                       //Batch Job id
                       "123",
                       // AWS endpoint URL
                       "--aws-endpoint-url", endpointUrl );
    }

    @Override
    protected void assertOnRecordings( Path recordingDir, List<ProfilerType> profilers, Resources resources ) throws Exception
    {
        Workload workload = Workload.fromName( "zero", resources, DEPLOYMENT );

        // should find at least one recording per profiler per benchmark -- there may be more, due to secondary recordings
        int profilerRecordingCount = (int) Files.list( recordingDir ).count();
        int minimumExpectedProfilerRecordingCount = profilers.size() * workload.queries().size();
        assertThat( profilerRecordingCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );

        for ( Query query : workload.queries() )
        {
            for ( ProfilerType profilerType : profilers )
            {
                ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                        query.benchmarkGroup(),
                        query.benchmark(),
                        RunPhase.MEASUREMENT,
                        profilerType,
                        Parameters.NONE );
                for ( RecordingType recordingType : profilerType.allRecordingTypes() )
                {
                    String profilerArtifactFilename = recordingDescriptor.filename( recordingType );
                    File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                    assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
                }
            }
        }
    }
}
