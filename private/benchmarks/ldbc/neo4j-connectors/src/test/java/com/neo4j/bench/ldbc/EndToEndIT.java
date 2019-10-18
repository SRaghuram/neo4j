/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingType;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.test.BaseEndToEndIT;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration.withoutWrites;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class EndToEndIT extends BaseEndToEndIT
{
    @Override
    protected String scriptName()
    {
        return "run-report-benchmark.sh";
    }

    @Override
    protected Path getJar( Path baseDir )
    {
        return baseDir.resolve( "neo4j-connectors/target/ldbc.jar" );
    }

    @Override
    protected List<String> processArgs( Resources resources,
                                        List<ProfilerType> profilers,
                                        String endpointUrl,
                                        Path baseDir,
                                        Jvm jvm,
                                        ResultStoreCredentials resultStoreCredentials ) throws IOException, DriverConfigurationException
    {
        // prepare neo4j config file
        Path baseNeo4jConfig = resources.getResourceFile( "/neo4j/neo4j_sf001.conf" );
        Path benchmarkNeo4jConfig = temporaryFolder.createFile( "neo4j_benchmark.config" ).toPath();
        Neo4jConfigBuilder.empty()
                          .setDense( true )
                          .writeToFile( benchmarkNeo4jConfig );

        Path resultsDir = temporaryFolder.directory( "results_dir" ).toPath();
        Path workDir = temporaryFolder.directory( "work" ).toPath();

        Path ldbcCsvDir = temporaryFolder.directory( "ldbc_sf001_p006_regular_utc" ).toPath();
        Path readParams = Files.createDirectory( ldbcCsvDir.resolve( "substitution_parameters" ) );
        Files.walkFileTree( resources.getResourceFile( "/validation_sets/data/substitution_parameters" ),
                            new CopyVisitor( readParams ) );
        Path writeParams = resources.getResourceFile( "/validation_sets/data/updates" );

        Path ldbcConfigFile = temporaryFolder.createFile( "ldbc_driver.conf" ).toPath();
        BenchmarkUtil.stringToFile( ldbcConfig().toPropertiesString(), ldbcConfigFile );

        Path dbPath = createLdbcStore( resources );

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
                       "tool_commit",
                       "tool_branch",
                       "tool_branch_owner",
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
                       "10",
                       // ldbc_run_count
                       "10",
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

    @Override
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
            ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                    benchmarkGroup,
                    benchmark,
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

    private Path createLdbcStore( Resources resources )
    {
        Path dbPath = temporaryFolder.directory( "db" ).toPath();
        Path csvData = resources.getResourceFile( "/validation_sets/data/social_network/num_date" );
        boolean createUniqueConstraints = true;
        boolean createMandatoryConstraints = true;
        LdbcCli.importParallelRegular( dbPath.toFile(),
                                       csvData.toFile(),
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
        int warmupCount = 10;
        long operationCount = 10;
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
                operationCount,
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
                warmupCount,
                skipCount
        );
        Map<String,String> readOnly = withoutWrites( DriverConfigUtils.ldbcSnbInteractive() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( readOnly );
        return configuration;
    }
}
