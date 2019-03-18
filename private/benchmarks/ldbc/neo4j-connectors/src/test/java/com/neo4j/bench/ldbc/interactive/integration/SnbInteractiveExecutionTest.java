/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.DbException;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.util.TestDirectorySupport;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.TestUtils;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.Scenario;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.client.util.TestDirectorySupport.createTempDirectory;
import static com.neo4j.bench.client.util.TestDirectorySupport.createTempFile;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
public abstract class SnbInteractiveExecutionTest
{
    static class DatabaseAndUrl implements AutoCloseable
    {
        private final GraphDatabaseService db;
        private final String url;

        DatabaseAndUrl( GraphDatabaseService db, String url )
        {
            this.db = db;
            this.url = url;
        }

        @Override
        public void close() throws Exception
        {
            if ( null != db )
            {
                db.shutdown();
            }
        }
    }

    private static final boolean CALCULATE_WORKLOAD_STATISTICS = false;
    private static final ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions
            CONSOLE_AND_FILE_VALIDATION_PARAM_OPTIONS = null;
    private static final String DATABASE_VALIDATION_FILE_PATH = null;
    private static final int STATUS_DISPLAY_INTERVAL_AS_SECONDS = 1;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final long SPINNER_SLEEP_DURATION_AS_MILLI = 1;
    private static final boolean PRINT_HELP = false;

    @Inject
    public TestDirectory temporaryFolder;

    abstract Scenario buildValidationData() throws DbException;

    DriverConfiguration modifyConfiguration( DriverConfiguration configuration ) throws DriverConfigurationException
    {
        return configuration;
    }

    DatabaseAndUrl createRemoteConnector( File dbDir )
    {
        return new DatabaseAndUrl( null, null );
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadOnlyWorkload() throws Exception
    {
        long skipCount = 0;
        long warmupCount = 10000;
        long operationCount = 10000;
        boolean ignoreScheduledStartTimes = false;
        doShouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedApi(
                ignoreScheduledStartTimes,
                skipCount,
                warmupCount,
                operationCount,
                buildValidationData()
        );
    }

    private void doShouldRunLdbcSnbInteractiveReadOnlyWorkloadWithEmbeddedApi(
            boolean ignoreScheduledStartTimes,
            long skipCount,
            long warmupCount,
            long operationCount,
            Scenario scenario ) throws Exception
    {
        File storeDir = createTempDirectory( temporaryFolder.absolutePath() );
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema(),
                scenario.neo4jImporter()
        ).load(
                storeDir.toPath().resolve( UUID.randomUUID().toString() ).toFile(),
                scenario.csvDir(),
                DriverConfigUtils.neo4jTestConfig(),
                scenario.csvDateFormat(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution(),
                true,
                false
        );
        Store store = Store.createFrom( storeDir.toPath() );
        File resultDir = createTempDirectory( temporaryFolder.absolutePath() );
        assertThat( resultDir.listFiles().length, is( 0 ) );

        try ( DatabaseAndUrl databaseAndUrl = createRemoteConnector( store.graphDbDirectory().toFile() ) )
        {
            int threadCount = 4;
            String resultDirPath = resultDir.getAbsolutePath();
            Double timeCompressionRatio = 0.0002;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    new HashMap<>(),
                    "LDBC-SNB",
                    Neo4jDb.class.getName(),
                    LdbcSnbInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    STATUS_DISPLAY_INTERVAL_AS_SECONDS,
                    TIME_UNIT,
                    resultDirPath,
                    timeCompressionRatio,
                    CONSOLE_AND_FILE_VALIDATION_PARAM_OPTIONS,
                    DATABASE_VALIDATION_FILE_PATH,
                    CALCULATE_WORKLOAD_STATISTICS,
                    SPINNER_SLEEP_DURATION_AS_MILLI,
                    PRINT_HELP,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount
            );
            Map<String,String> ldbcSnbInteractiveReadOnlyConfiguration =
                    LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                            DriverConfigUtils.ldbcSnbInteractive()
                    );
            configuration =
                    (ConsoleAndFileDriverConfiguration) configuration
                            .applyArgs( ldbcSnbInteractiveReadOnlyConfiguration );
            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                    Neo4jDb.neo4jConnectorPropertiesFor(
                            scenario.neo4jApi(),
                            scenario.planner(),
                            scenario.runtime(),
                            scenario.neo4jSchema(),
                            store.graphDbDirectory().toFile(),
                            DriverConfigUtils.neo4jTestConfig(),
                            LdbcSnbInteractiveWorkload.class,
                            databaseAndUrl.url
                    )
            );
            Map<String,String> additionalParameters = new HashMap<>();
            additionalParameters.put(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    scenario.paramsDir().getAbsolutePath() );
            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( additionalParameters );

            configuration = (ConsoleAndFileDriverConfiguration) modifyConfiguration( configuration );

            File ldbcConfigFile = createTempFile( temporaryFolder.absolutePath() );
            FileUtils.writeStringToFile( ldbcConfigFile, configuration.toPropertiesString() );
            LdbcCli.benchmark(
                    store,
                    scenario.updatesDir(),
                    scenario.paramsDir(),
                    resultDir,
                    scenario.neo4jApi(),
                    ldbcConfigFile,
                    DriverConfigUtils.neo4jTestConfig(),
                    threadCount
            );

            ResultsDirectory resultsDirectory = new ResultsDirectory( configuration );
            for ( File file : resultsDirectory.expectedFiles() )
            {
                assertTrue( file.exists(),
                            format( "Expected file to exist: %s\nOnly found: %s",
                                    file.getAbsolutePath(),
                                    resultsDirectory.files().stream().map( File::getName ).collect( toList() ) ) );
            }

            assertTrue( resultsDirectory.files().containsAll( resultsDirectory.expectedFiles() ),
                        format( "Expected that: %s\nWill contain: %s",
                                resultsDirectory.files(),
                                resultsDirectory.expectedFiles() ) );

            long actualOperationCount = resultsDirectory.getResultsLogFileLength( false );
            assertThat( "Operation count = " + actualOperationCount,
                    actualOperationCount,
                    allOf(
                            greaterThanOrEqualTo( TestUtils.operationCountLower( configuration.operationCount()
                            ) ),
                            lessThanOrEqualTo( TestUtils.operationCountUpper( configuration.operationCount() ) )
                    )
            );
        }
    }

    @Test
    public void shouldRunLdbcSnbInteractiveWriteOnlyWorkload() throws Exception
    {
        long skipCount = 0;
        long warmupCount = 15000;
        long operationCount = 15000;
        boolean ignoreScheduledStartTimes = false;
        doShouldRunLdbcSnbInteractiveWriteOnlyWorkloadWithEmbeddedApi(
                ignoreScheduledStartTimes,
                skipCount,
                warmupCount,
                operationCount,
                buildValidationData()
        );
    }

    private void doShouldRunLdbcSnbInteractiveWriteOnlyWorkloadWithEmbeddedApi(
            boolean ignoreScheduledStartTimes,
            long skipCount,
            long warmupCount,
            long operationCount,
            Scenario scenario ) throws Exception
    {
        File storeDir = createTempDirectory( temporaryFolder.absolutePath() );
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema(),
                scenario.neo4jImporter()
        ).load(
                storeDir.toPath().resolve( UUID.randomUUID().toString() ).toFile(),
                scenario.csvDir(),
                DriverConfigUtils.neo4jTestConfig(),
                scenario.csvDateFormat(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution(),
                true,
                false
        );
        File resultDir = createTempDirectory( temporaryFolder.absolutePath() );
        Store store = Store.createFrom( storeDir.toPath() );
        assertThat( resultDir.listFiles().length, is( 0 ) );

        try ( DatabaseAndUrl databaseAndUrl = createRemoteConnector( store.graphDbDirectory().toFile() ) )
        {
            int threadCount = 4;
            String resultDirPath = resultDir.getAbsolutePath();
            Double timeCompressionRatio = 0.00015;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    new HashMap<>(),
                    "LDBC-SNB",
                    Neo4jDb.class.getName(),
                    LdbcSnbInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    STATUS_DISPLAY_INTERVAL_AS_SECONDS,
                    TIME_UNIT,
                    resultDirPath,
                    timeCompressionRatio,
                    CONSOLE_AND_FILE_VALIDATION_PARAM_OPTIONS,
                    DATABASE_VALIDATION_FILE_PATH,
                    CALCULATE_WORKLOAD_STATISTICS,
                    SPINNER_SLEEP_DURATION_AS_MILLI,
                    PRINT_HELP,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount
            );

            Map<String,String> ldbcSnbInteractiveConfiguration =
                    LdbcSnbInteractiveWorkloadConfiguration.withoutShortReads(
                            LdbcSnbInteractiveWorkloadConfiguration.withoutLongReads(
                                    DriverConfigUtils.ldbcSnbInteractive()
                            )
                    );
            configuration =
                    (ConsoleAndFileDriverConfiguration) configuration.applyArgs( ldbcSnbInteractiveConfiguration );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                    Neo4jDb.neo4jConnectorPropertiesFor(
                            scenario.neo4jApi(),
                            scenario.planner(),
                            scenario.runtime(),
                            scenario.neo4jSchema(),
                            store.graphDbDirectory().toFile(),
                            DriverConfigUtils.neo4jTestConfig(),
                            LdbcSnbInteractiveWorkload.class,
                            databaseAndUrl.url
                    )
            );

            Map<String,String> additionalParameters = new HashMap<>();
            additionalParameters.put(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    scenario.paramsDir().getAbsolutePath() );
            additionalParameters.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    scenario.updatesDir().getAbsolutePath() );
            additionalParameters.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER,
                    LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.CHAR_SEEKER.name() );
            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( additionalParameters );

            Map<String,String> ldbcSnbInteractiveReadOnlyConfiguration = MapUtils.loadPropertiesToMap(
                    new File( scenario.updatesDir(), "updateStream.properties" ) );
            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( ldbcSnbInteractiveReadOnlyConfiguration );

            configuration = (ConsoleAndFileDriverConfiguration) modifyConfiguration( configuration );

            File ldbcConfigFile = createTempFile( temporaryFolder.absolutePath() );
            FileUtils.writeStringToFile( ldbcConfigFile, configuration.toPropertiesString(), StandardCharsets.UTF_8 );
            LdbcCli.benchmark(
                    store,
                    scenario.updatesDir(),
                    scenario.paramsDir(),
                    resultDir,
                    scenario.neo4jApi(),
                    ldbcConfigFile,
                    DriverConfigUtils.neo4jTestConfig(),
                    threadCount
            );

            ResultsDirectory resultsDirectory = new ResultsDirectory( configuration );
            for ( File file : resultsDirectory.expectedFiles() )
            {
                assertTrue( file.exists(),
                            format( "Expected file to exist: %s\nOnly found: %s",
                                    file.getAbsolutePath(),
                                    resultsDirectory.files().stream().map( File::getName ).collect( toList() ) ) );
            }

            assertTrue( resultsDirectory.files().containsAll( resultsDirectory.expectedFiles() ),
                        format( "Expected that: %s\nWill contain: %s",
                                resultsDirectory.files(),
                                resultsDirectory.expectedFiles() ) );

            long actualOperationCount = resultsDirectory.getResultsLogFileLength( false );
            assertThat( actualOperationCount,
                    is( configuration.operationCount() + 1 ) ); // + 1 to account for csv headers
            assertThat( "Operation count = " + actualOperationCount,
                    actualOperationCount,
                    allOf(
                            greaterThanOrEqualTo( TestUtils.operationCountLower( configuration.operationCount() ) ),
                            lessThanOrEqualTo( TestUtils.operationCountUpper( configuration.operationCount() ) )
                    )
            );
        }
    }

    @Test
    public void shouldRunLdbcSnbInteractiveReadWriteWorkloadWithEmbeddedApi() throws Exception
    {
        long skipCount = 0;
        long warmupCount = 15000;
        long operationCount = 15000;
        boolean ignoreScheduledStartTimes = false;
        doShouldRunLdbcSnbInteractiveReadWriteWorkloadWithEmbeddedApi(
                ignoreScheduledStartTimes,
                skipCount,
                warmupCount,
                operationCount,
                buildValidationData()
        );
    }

    private void doShouldRunLdbcSnbInteractiveReadWriteWorkloadWithEmbeddedApi(
            boolean ignoreScheduledStartTimes,
            long skipCount,
            long warmupCount,
            long operationCount,
            Scenario scenario ) throws Exception
    {
        File storeDir = createTempDirectory( temporaryFolder.absolutePath() );
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema(),
                scenario.neo4jImporter()
        ).load(
                storeDir.toPath().resolve( UUID.randomUUID().toString() ).toFile(),
                scenario.csvDir(),
                DriverConfigUtils.neo4jTestConfig(),
                scenario.csvDateFormat(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution(),
                true,
                false
        );
        File resultDir = createTempDirectory( temporaryFolder.absolutePath() );
        Store store = Store.createFrom( storeDir.toPath() );
        assertThat( resultDir.listFiles().length, is( 0 ) );

        try ( DatabaseAndUrl databaseAndUrl = createRemoteConnector( store.graphDbDirectory().toFile() ) )
        {
            int threadCount = 4;
            String resultDirPath = resultDir.getAbsolutePath();
            Double timeCompressionRatio = 0.00035;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    new HashMap<>(),
                    "LDBC-SNB",
                    Neo4jDb.class.getName(),
                    LdbcSnbInteractiveWorkload.class.getName(),
                    operationCount,
                    threadCount,
                    STATUS_DISPLAY_INTERVAL_AS_SECONDS,
                    TIME_UNIT,
                    resultDirPath,
                    timeCompressionRatio,
                    CONSOLE_AND_FILE_VALIDATION_PARAM_OPTIONS,
                    DATABASE_VALIDATION_FILE_PATH,
                    CALCULATE_WORKLOAD_STATISTICS,
                    SPINNER_SLEEP_DURATION_AS_MILLI,
                    PRINT_HELP,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount
            );

            Map<String,String> ldbcSnbInteractiveConfiguration = DriverConfigUtils.ldbcSnbInteractive();
            configuration =
                    (ConsoleAndFileDriverConfiguration) configuration.applyArgs( ldbcSnbInteractiveConfiguration );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                    Neo4jDb.neo4jConnectorPropertiesFor(
                            scenario.neo4jApi(),
                            scenario.planner(),
                            scenario.runtime(),
                            scenario.neo4jSchema(),
                            store.graphDbDirectory().toFile(),
                            DriverConfigUtils.neo4jTestConfig(),
                            LdbcSnbInteractiveWorkload.class,
                            databaseAndUrl.url
                    )
            );

            Map<String,String> additionalParameters = new HashMap<>();
            additionalParameters.put(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    scenario.paramsDir().getAbsolutePath() );
            additionalParameters.put(
                    LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    scenario.updatesDir().getAbsolutePath() );
            additionalParameters
                    .put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( operationCount ) );
            additionalParameters.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER,
                    LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.CHAR_SEEKER.name() );
            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( additionalParameters );

            Map<String,String> ldbcSnbInteractiveReadOnlyConfiguration =
                    MapUtils.loadPropertiesToMap( new File( scenario.updatesDir(), "updateStream.properties" ) );
            configuration =
                    (ConsoleAndFileDriverConfiguration) configuration
                            .applyArgs( ldbcSnbInteractiveReadOnlyConfiguration );

            configuration = (ConsoleAndFileDriverConfiguration) modifyConfiguration( configuration );

            File ldbcConfigFile = createTempFile( temporaryFolder.absolutePath() );
            FileUtils.writeStringToFile( ldbcConfigFile, configuration.toPropertiesString(), StandardCharsets.UTF_8 );
            LdbcCli.benchmark(
                    store,
                    scenario.updatesDir(),
                    scenario.paramsDir(),
                    resultDir,
                    scenario.neo4jApi(),
                    ldbcConfigFile,
                    DriverConfigUtils.neo4jTestConfig(),
                    threadCount
            );

            ResultsDirectory resultsDirectory = new ResultsDirectory( configuration );
            for ( File file : resultsDirectory.expectedFiles() )
            {
                assertTrue( file.exists(),
                            format( "Expected file to exist: %s\nOnly found: %s",
                                    file.getAbsolutePath(),
                                    resultsDirectory.files().stream().map( File::getName ).collect( toList() ) ));
            }

            assertTrue( resultsDirectory.files().containsAll( resultsDirectory.expectedFiles() ),
                    format( "Expected that: %s\nWill contain: %s",
                            resultsDirectory.files(),
                            resultsDirectory.expectedFiles() )
            );

            long actualOperationCount = resultsDirectory.getResultsLogFileLength( false );
            assertThat( "Operation count = " + actualOperationCount,
                    actualOperationCount,
                    allOf(
                            greaterThanOrEqualTo( TestUtils.operationCountLower( configuration.operationCount() ) ),
                            lessThanOrEqualTo( TestUtils.operationCountUpper( configuration.operationCount() ) )
                    )
            );
        }
    }
}
