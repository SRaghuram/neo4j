/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.integration;

import com.ldbc.driver.Client;
import com.ldbc.driver.client.ClientMode;
import com.ldbc.driver.client.ValidateDatabaseMode;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.validation.DbValidationResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.Scenario;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Disabled
@TestDirectoryExtension
class IntegrationValidationTest
{
    private static final Logger LOG = LoggerFactory.getLogger( IntegrationValidationTest.class );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldCreatePublicValidationSet() throws Exception
    {
        File validationSetDir = DriverConfigUtils.getResource( "/validation_sets/neo4j/business_intelligence/" );
        File dataDir = DriverConfigUtils.getResource( "/validation_sets/data/" );
        doShouldCreatePublicValidationSet(
                new Scenario(
                        new File( dataDir, "social_network/string_date/" ),
                        new File( dataDir, "substitution_parameters/" ),
                        new File( dataDir, "updates/" ),
                        new File( validationSetDir, "validation_params.csv" ),
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Neo4jApi.EMBEDDED_CYPHER,
                        Planner.DEFAULT,
                        Runtime.DEFAULT,
                        LdbcDateCodec.Format.STRING_ENCODED,
                        LdbcDateCodec.Format.NUMBER_ENCODED,
                        LdbcDateCodec.Resolution.NOT_APPLICABLE
                )
        );
    }

    private void doShouldCreatePublicValidationSet( Scenario scenario ) throws Exception
    {
        File storeDir;

        /*
        CREATE VALIDATION PARAMETERS FOR USE IN VALIDATING OTHER IMPLEMENTATIONS
         */

        if ( scenario.validationParamsFile().exists() )
        {
            FileUtils.forceDelete( scenario.validationParamsFile() );
        }
        scenario.validationParamsFile().createNewFile();
        int validationSetSize = 1000;

        assertThat( scenario.validationParamsFile().length() == 0, is( true ) );

        long operationCount = 10000;
        int threadCount = 4;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions(
                        scenario.validationParamsFile().getAbsolutePath(),
                        validationSetSize
                );
        String databaseValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 1;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 0;
        long skipCount = 0;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                operationCount,
                threadCount,
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                validationCreationParams,
                databaseValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( DriverConfigUtils.ldbcSnbBi() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArg(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                scenario.paramsDir().getAbsolutePath()
        );

        storeDir = temporaryFolder.directory( "store" );

        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
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

        Store store = Neo4jStore.createFrom( storeDir.toPath() );

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

        LOG.debug( configuration.toPropertiesString() );

        TimeSource timeSource = new SystemTimeSource();
        long workloadStartTimeAsMilli = timeSource.nowAsMilli() + TimeUnit.SECONDS.toMillis( 1 );
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        ControlService controlService = new LocalControlService(
                workloadStartTimeAsMilli,
                configuration,
                loggingServiceFactory,
                timeSource
        );
        Client client = new Client();
        ClientMode clientMode = client.getClientModeFor( controlService );
        clientMode.init();
        clientMode.startExecutionAndAwaitCompletion();

        assertThat( scenario.validationParamsFile().length() >= validationSetSize, is( true ) );
    }

    @Test
    void shouldValidateAgainstPublicNeo4jValidationSetApi() throws Exception
    {
        doShouldValidateAgainstPublicValidationSetApi(
                Scenario.randomBi()
        );
    }

    private void doShouldValidateAgainstPublicValidationSetApi( Scenario scenario ) throws Exception
    {
        assertThat( scenario.validationParamsFile().length() > 0, is( true ) );

        long operationCount = 100000;
        int threadCount = 4;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.directory( "results" ).toString();

        Double timeCompressionRatio = 1.0;
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
        String databaseValidationFilePath = scenario.validationParamsFile().getAbsolutePath();
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 0;
        long skipCount = 0;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                operationCount,
                threadCount,
                statusDisplayIntervalAsSeconds,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                validationCreationParams,
                databaseValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( DriverConfigUtils.ldbcSnbBi() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArg(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                scenario.paramsDir().getAbsolutePath()
        );

        /*
        VALIDATE EMBEDDED API
         */

        File dbDir = temporaryFolder.directory( "db" );
        File configFile = DriverConfigUtils.neo4jTestConfig( temporaryFolder.directory( "config" ) );
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema()
        ).load(
                dbDir,
                scenario.csvDir(),
                configFile,
                scenario.csvDateFormat(),
                scenario.neo4jDateFormat(),
                scenario.timestampResolution(),
                true,
                false
        );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                Neo4jDb.neo4jConnectorPropertiesFor(
                        scenario.neo4jApi(),
                        scenario.planner(),
                        scenario.runtime(),
                        scenario.neo4jSchema(),
                        dbDir,
                        configFile,
                        LdbcSnbBiWorkload.class,
                        null
                )
        );

        LOG.debug( configuration.toPropertiesString() );

        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( false );
        ControlService controlService = new LocalControlService(
                System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 1 ),
                configuration,
                loggingServiceFactory,
                new SystemTimeSource()
        );
        Client client = new Client();
        ClientMode clientMode = client.getClientModeFor( controlService );
        clientMode.init();
        DbValidationResult dbValidationResult = ((ValidateDatabaseMode) clientMode).startExecutionAndAwaitCompletion();

        assertThat( dbValidationResult.isSuccessful(), is( true ) );
    }
}
