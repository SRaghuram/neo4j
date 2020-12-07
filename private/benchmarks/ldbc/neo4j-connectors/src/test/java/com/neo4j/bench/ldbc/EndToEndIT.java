/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.test.BaseEndToEndIT;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY;
import static com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration.withoutWrites;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

public class EndToEndIT extends BaseEndToEndIT
{

    /**
     * These numbers should be large enough so JFR can manage to save method sampling events, otherwise flamegraphs generation will fail. With these values
     * benchmarks take ~10 seconds on a Ryzen 4800H CPU.
     */
    private static final int WARMUP_COUNT = 15_000;
    private static final int OPERATION_COUNT = WARMUP_COUNT * 2;

    @Test
    @Tag( "endtoend" )
    public void runReportBenchmark() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );

        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories(resourcesPath);
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers ),
                                 this::assertOnRecordings,
                                 1 );
        }
    }

    protected String scriptName()
    {
        return "run-report-benchmark.sh";
    }

    protected Path getJar()
    {
        return Paths.get( "neo4j-connectors/target/ldbc.jar" );
    }

    protected List<String> processArgs( Resources resources,
                                        List<ProfilerType> profilers ) throws IOException, DriverConfigurationException
    {
        ResultStoreCredentials resultStoreCredentials = getResultStoreCredentials();
        Jvm jvm = Jvm.defaultJvmOrFail();
        String endpointUrl = getAWSEndpointURL();
        // prepare neo4j config file
        Path baseNeo4jConfig = resources.getResourceFile( "/neo4j/neo4j_sf001.conf" );
        Path benchmarkNeo4jConfig = temporaryFolder.resolve( "neo4j_benchmark.config" );
        Neo4jConfigBuilder.withDefaults()
                          .setDense( true )
                          .writeToFile( benchmarkNeo4jConfig );

        Path resultsDir = temporaryFolder.resolve( "results_dir" );
        Path workDir = temporaryFolder.resolve( "work" );
        Files.createDirectories( resultsDir );
        Files.createDirectories( workDir );
        Path ldbcCsvDir = temporaryFolder.resolve( "ldbc_sf001_p006_regular_utc" );
        Files.createDirectories(ldbcCsvDir);
        Path readParams = Files.createDirectory( ldbcCsvDir.resolve( "substitution_parameters" ) );
        Files.walkFileTree( resources.getResourceFile( "/validation_sets/data/substitution_parameters" ),
                            new CopyVisitor( readParams ) );
        Path writeParams = resources.getResourceFile( "/validation_sets/data/updates" );

        Path ldbcConfigFile = temporaryFolder.resolve( "ldbc_driver.conf" );

        BenchmarkUtil.stringToFile( ldbcConfig().toPropertiesString(), ldbcConfigFile );

        Path dbPath = createLdbcStore( resources, benchmarkNeo4jConfig );

        return asList( "./" + scriptName(),
                       "3.5.1",
                       "tool_commit",
                       "neo4j_branch",
                       "neo4j_branch_owner",
                       Neo4jApi.EMBEDDED_CYPHER.name(),
                       Planner.DEFAULT.name(),
                       Runtime.DEFAULT.name(),
                       // neo4j_config
                       baseNeo4jConfig.toString(),
                       // neo4j_benchmark_config
                       benchmarkNeo4jConfig.toString(),
                       // teamcity_build
                       "1",
                       // parent_teamcity_build
                       "0",
                       resultStoreCredentials.boltUri(),
                       resultStoreCredentials.user(),
                       resultStoreCredentials.pass(),
                       // ldbc_read_params
                       readParams.toAbsolutePath().toString(),
                       // ldbc_write_params
                       writeParams.toAbsolutePath().toString(),
                       // ldbc_config
                       ldbcConfigFile.toAbsolutePath().toString(),
                       // ldbc_read_threads
                       "8",
                       // ldbc_warmup_count
                       Integer.toString( WARMUP_COUNT ),
                       // ldbc_run_count
                       Integer.toString( OPERATION_COUNT ),
                       // ldbc_repetition_count (a.k.a fork count)
                       "1",
                       // ldbc_results_dir
                       resultsDir.toAbsolutePath().toString(),
                       // ldbc_working_dir
                       workDir.toAbsolutePath().toString(),
                       // ldbc_source_db
                       dbPath.toAbsolutePath().toString(),
                       // ldbc_db_reuse_policy
                       "REUSE",
                       // ldbc_jvm_args
                       "-Xmx1g",
                       // jvm_path
                       jvm.launchJava(),
                       // profilers
                       profilers.stream().map( ProfilerType::name ).collect( joining( "," ) ),
                       "triggered_by",
                       // AWS endpoint URL
                       endpointUrl );
    }

    protected void assertOnRecordings( Path recordingDir, List<ProfilerType> profilers, Resources resources ) throws Exception
    {
        // should find at least one recording per profiler per benchmark (LDBC is 1 benchmark) -- there may be more, due to secondary recordings
        int profilerRecordingCount = (int) Files.list( recordingDir ).count();
        int minimumExpectedProfilerRecordingCount = profilers.size();
        assertThat( profilerRecordingCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );

        BenchmarkGroup benchmarkGroup = new BenchmarkGroup( "LdbcSnbInteractive-Read" );
        Map<String,String> benchmarkParams = new HashMap<>();
        benchmarkParams.put( "api", "EMBEDDED_CYPHER" );
        benchmarkParams.put( "planner", "DEFAULT" );
        benchmarkParams.put( "runtime", "DEFAULT" );
        benchmarkParams.put( "store_format", "STANDARD" );
        benchmarkParams.put( "scale_factor", "1" );
        Benchmark benchmark = Benchmark.benchmarkFor( "Summary metrics of entire workload",
                                                      "Summary",
                                                      Benchmark.Mode.THROUGHPUT,
                                                      benchmarkParams );

        for ( ProfilerType profilerType : profilers )
        {
            ProfilerRecordingDescriptor profilerRecordingDescriptor = ProfilerRecordingDescriptor.create(
                    benchmarkGroup,
                    benchmark,
                    RunPhase.MEASUREMENT,
                    ParameterizedProfiler.defaultProfiler( profilerType ),
                    Parameters.NONE );
            for ( RecordingType recordingType : profilerType.allRecordingTypes() )
            {
                RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( recordingType );
                String profilerArtifactFilename = recordingDescriptor.filename();
                File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
            }
        }
    }

    private static class CopyVisitor extends SimpleFileVisitor<Path>
    {
        private final Path toPath;

        private CopyVisitor( Path toPath )
        {
            this.toPath = toPath;
        }

        @Override
        public FileVisitResult preVisitDirectory( Path dir, BasicFileAttributes attrs )
        {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
        {
            Files.copy( file, toPath.resolve( file.getFileName() ) );
            return FileVisitResult.CONTINUE;
        }
    }

    private Path createLdbcStore( Resources resources, Path benchmarkNeo4jConfig ) throws IOException
    {
        Path dbPath = temporaryFolder.resolve( "db" );
        Path csvData = resources.getResourceFile( "/validation_sets/data/social_network/num_date" );
        boolean createUniqueConstraints = true;
        boolean createMandatoryConstraints = true;
        LdbcCli.importParallelRegular( dbPath.toFile(),
                                       csvData.toFile(),
                                       benchmarkNeo4jConfig.toFile(),
                                       createUniqueConstraints,
                                       createMandatoryConstraints,
                                       LdbcDateCodec.Format.NUMBER_UTC,
                                       LdbcDateCodec.Format.NUMBER_UTC );
        return dbPath;
    }

    private DriverConfiguration ldbcConfig() throws IOException, DriverConfigurationException
    {
        int threadCount = 1;
        double timeCompressionRatio = 1.0;
        int statusDisplayIntervalAsSeconds = 1;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDurationAsMilli = 1L;
        boolean ignoreScheduledStartTimes = true;
        int skipCount = 0;
        String resultDirPath = null;
        boolean printHelp = false;
        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                OPERATION_COUNT,
                threadCount,
                statusDisplayIntervalAsSeconds,
                MILLISECONDS,
                resultDirPath,
                timeCompressionRatio,
                null,
                null,
                calculateWorkloadStatistics,
                spinnerSleepDurationAsMilli,
                printHelp,
                ignoreScheduledStartTimes,
                WARMUP_COUNT,
                skipCount
        );
        Map<String,String> readOnly = withoutWrites( DriverConfigUtils.ldbcSnbInteractive() );
        // a single LdbcQuery14 can take even 90 seconds to finish, and other read queries should be enough to cover this test case
        readOnly.put( LONG_READ_OPERATION_14_ENABLE_KEY, "false" );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( readOnly );
        return configuration;
    }
}
