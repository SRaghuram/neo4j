/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.integration;

import com.ldbc.driver.DbException;
import com.ldbc.driver.client.ResultsDirectory;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.TestUtils;
import com.neo4j.bench.ldbc.cli.LdbcCli;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.Scenario;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public abstract class SnbBiExecutionTest
{
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

    @Test
    public void shouldRunLdbcSnbBiWorkloadAndEmbeddedApi() throws Exception
    {
        long skipCount = 1000;
        long warmupCount = 1000;
        long operationCount = 1000;
        boolean ignoreScheduledStartTimes = true;
        doShouldRunLdbcSnbBiWorkloadWithEmbeddedApi(
                ignoreScheduledStartTimes,
                skipCount,
                warmupCount,
                operationCount,
                buildValidationData()
        );
    }

    private void doShouldRunLdbcSnbBiWorkloadWithEmbeddedApi(
            boolean ignoreScheduledStartTimes,
            long skipCount,
            long warmupCount,
            long operationCount,
            Scenario scenario ) throws Exception
    {
        File storeDir = temporaryFolder.directory( "store" ).toFile();
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ).toFile() );
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema()
        ).load(
                storeDir,
                scenario.csvDir(),
                configFile,
                scenario.csvDateFormat(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution(),
                true,
                false
        );
        File resultDir = temporaryFolder.directory( "results" ).toFile();
        Store store = Neo4jStore.createFrom( storeDir.toPath() );
        assertThat( resultDir.listFiles().length, is( 0 ) );

        int threadCount = 1;
        String resultDirPath = resultDir.getAbsolutePath();
        Double timeCompressionRatio = 1.0;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
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

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( DriverConfigUtils.ldbcSnbBi() );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                Neo4jDb.neo4jConnectorPropertiesFor(
                        scenario.neo4jApi(),
                        scenario.planner(),
                        scenario.runtime(),
                        scenario.neo4jSchema(),
                        store.topLevelDirectory().toFile(),
                        configFile,
                        LdbcSnbBiWorkload.class,
                        null
                )
        );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArg(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                scenario.paramsDir().getAbsolutePath()
        );

        // TODO remove
        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_1_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_2_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_3_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_4_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_5_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_6_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_7_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_8_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_9_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_10_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_11_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_12_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_13_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_14_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_15_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_16_ENABLE_KEY,
                        "false"
                ).applyArg(
                        // TODO recreate datasets with new shorter variable depth
                        // TODO enable
                        LdbcSnbBiWorkloadConfiguration.OPERATION_17_ENABLE_KEY,
                        "false"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_18_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_19_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_20_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_21_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_22_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_23_ENABLE_KEY,
                        "true"
                ).applyArg(
                        LdbcSnbBiWorkloadConfiguration.OPERATION_24_ENABLE_KEY,
                        "true"
                ).applyArg(
                        // TODO enable
                        LdbcSnbBiWorkloadConfiguration.OPERATION_25_ENABLE_KEY,
                        "false"
                );

        File ldbcConfigFile = temporaryFolder.file( "ldbc.conf" ).toFile();
        BenchmarkUtil.stringToFile( configuration.toPropertiesString(), ldbcConfigFile.toPath() );
        LdbcCli.benchmark(
                store,
                scenario.updatesDir(),
                scenario.paramsDir(),
                resultDir,
                scenario.neo4jApi(),
                ldbcConfigFile,
                configFile,
                threadCount
        );

        ResultsDirectory resultsDirectory = new ResultsDirectory( configuration );
        for ( File file : resultsDirectory.expectedFiles() )
        {
            assertTrue( file.exists(),
                        format( "Expected file to exist: %s\nOnly found: %s", file.getAbsolutePath(),
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
                            greaterThanOrEqualTo( TestUtils.operationCountLower( configuration.operationCount() ) ),
                            lessThanOrEqualTo( TestUtils.operationCountUpper( configuration.operationCount() ) )
                    )
        );
    }
}
