/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.micro.benchmarks.test.NoOpBenchmark;
import com.neo4j.bench.test.BaseEndToEndIT;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EndToEndIT extends BaseEndToEndIT
{
    private AnnotationsFixture annotationsFixture = new AnnotationsFixture();

    @Test
    public void runReportBenchmark() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );

        runReportBenchmarks( scriptName(),
                             getJar(),
                             profilers,
                             processArgs( profilers,
                                          getAWSEndpointURL(),
                                          Jvm.defaultJvmOrFail(),
                                          getResultStoreCredentials() ),
                             this::assertOnRecordings,
                             1,
                             ExpectedRecordings.from( profilers ) );
    }

    protected String scriptName()
    {
        return "run-report-benchmarks.sh";
    }

    protected Path getJar()
    {
        return Paths.get( "target/micro-benchmarks.jar" );
    }

    protected List<String> processArgs( List<ProfilerType> profilers,
                                        String endpointUrl,
                                        Jvm jvm,
                                        ResultStoreCredentials resultStoreCredentials )
    {
        // prepare neo4j config file
        Path neo4jConfig = temporaryFolder.resolve( "neo4j.config" );
        Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

        File benchmarkConfig = createBenchmarkConfig( temporaryFolder );

        File workDir = temporaryFolder.resolve( "work_dir" ).toFile();

        return asList( "./" + scriptName(),
                       // neo4j_version
                       "3.3.0",
                       // neo4j_commit
                       "neo4j_commit",
                       // neo4j_branch
                       "neo4j_branch",
                       // neo4j_branch_owner
                       "neo4j_branch_owner",
                       resultStoreCredentials.boltUri(),
                       resultStoreCredentials.user(),
                       resultStoreCredentials.pass(),
                       // benchmark_config
                       benchmarkConfig.toString(),
                       // teamcity_build_id
                       "0",
                       // parent_teamcity_build_id
                       "1",
                       // jvm_args
                       "",
                       // jmh_args
                       "-f 1 -wi 1 -i 1",
                       // neo4j_config_path
                       neo4jConfig.toString(),
                       // jvm_path
                       jvm.launchJava(),
                       // profilers
                       ProfilerType.serializeProfilers( profilers ),
                       // triggered_by
                       "triggered_by",
                       // work_dir
                       workDir.getAbsolutePath(),
                       "--aws-endpoint-url", endpointUrl );
    }

    protected void assertOnRecordings( Path recordingDir, List<ProfilerType> profilers ) throws Exception
    {
        // all recordings
        List<Path> recordings = Files.list( recordingDir ).collect( Collectors.toList() );

        // all expected recordings
        long expectedRecordingsCount = profilers.stream()
                                                .mapToLong( profiler -> profiler.allRecordingTypes().size() )
                                                .sum();

        long existingRecordingsCount = profilers.stream()
                                                .flatMap( profiler -> profiler.allRecordingTypes().stream() )
                                                .filter( recording ->
                                                                 recordings.stream()
                                                                           .map( file -> file.getFileName().toString() )
                                                                           .anyMatch( filename -> filename.endsWith( recording.extension() ) ) )
                                                .count();

        assertEquals( expectedRecordingsCount, existingRecordingsCount, "number of existing recordings differs from expected number of recordings" );
    }

    private File createBenchmarkConfig( Path temporaryFolder )
    {
        File benchmarkConfig = temporaryFolder.resolve( "benchmarkConfig" ).toFile();

        Validation validation = new Validation();
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( annotationsFixture.getAnnotations(), validation );
        Validation.assertValid( validation );
        BenchmarkConfigFile.write(
                suiteDescription,
                ImmutableSet.of( NoOpBenchmark.class.getName() ),
                false,
                false,
                benchmarkConfig.toPath() );
        return benchmarkConfig;
    }
}
