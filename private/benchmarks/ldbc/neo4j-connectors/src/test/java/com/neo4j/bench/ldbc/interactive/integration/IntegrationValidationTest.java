/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.Client;
import com.ldbc.driver.client.ClientMode;
import com.ldbc.driver.client.ValidateDatabaseMode;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.validation.DbValidationResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
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
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.ldbc.DriverConfigUtils.getResource;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@TestDirectoryExtension
class IntegrationValidationTest
{
    private static final Logger LOG = LoggerFactory.getLogger( IntegrationValidationTest.class );

    @Inject
    private TestDirectory temporaryFolder;

    @Test
    void shouldCreatePublicValidationSet() throws Exception
    {
        File validationSetDir = getResource( "/validation_sets/neo4j/interactive/" );
        File dataDir = getResource( "/validation_sets/data/" );
        doShouldCreatePublicValidationSet(
                new Scenario(
                        new File( dataDir, "social_network/string_date/" ),
                        new File( dataDir, "substitution_parameters/" ),
                        new File( dataDir, "updates/" ),
                        new File( validationSetDir, "validation_params.csv" ),
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Neo4jApi.EMBEDDED_CORE,
                        Planner.DEFAULT,
                        Runtime.DEFAULT,
                        LdbcDateCodec.Format.STRING_ENCODED,
                        LdbcDateCodec.Format.NUMBER_UTC,
                        LdbcDateCodec.Resolution.NOT_APPLICABLE
                )
        );
    }

    private void doShouldCreatePublicValidationSet( Scenario scenario ) throws Exception
    {
        /*
        CREATE VALIDATION PARAMETERS FOR USE IN VALIDATING OTHER IMPLEMENTATIONS
         */

        if ( scenario.validationParamsFile().exists() )
        {
            FileUtils.forceDelete( scenario.validationParamsFile() );
        }
        scenario.validationParamsFile().createNewFile();
        int validationSetSize = 500;

        assertThat( scenario.validationParamsFile().length() == 0, is( true ) );

        long operationCount = 100000;
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
        long spinnerSleepDuration = 0;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 0;
        long skipCount = 0;

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                new HashMap<>(),
                "LDBC-SNB",
                Neo4jDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
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

        Map<String,String> ldbcSnbInteractiveConfiguration = DriverConfigUtils.ldbcSnbInteractive();
        ldbcSnbInteractiveConfiguration.put(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                scenario.paramsDir().getAbsolutePath() );
        ldbcSnbInteractiveConfiguration.put(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                scenario.updatesDir().getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( ldbcSnbInteractiveConfiguration );
        Map<String,String> updateStreamConfiguration =
                MapUtils.loadPropertiesToMap( new File( scenario.updatesDir(), "updateStream.properties" ) );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( updateStreamConfiguration );

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

        Store store = Neo4jStore.createFrom( storeDir.toPath() );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                Neo4jDb.neo4jConnectorPropertiesFor(
                        scenario.neo4jApi(),
                        scenario.planner(),
                        scenario.runtime(),
                        scenario.neo4jSchema(),
                        store.topLevelDirectory().toFile(),
                        configFile,
                        LdbcSnbInteractiveWorkload.class,
                        null
                )
        );

        exportAnonymizedConfiguration(
                configuration,
                new File(
                        scenario.validationParamsFile().getParent(),
                        "ldbc_driver_config--validation_parameter_creation.properties"
                )
        );

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
        ClientMode<?> clientMode = client.getClientModeFor( controlService );
        clientMode.init();
        clientMode.startExecutionAndAwaitCompletion();

        assertThat( scenario.validationParamsFile().length() >= validationSetSize, is( true ) );
    }

    @Test
    void shouldValidateAgainstPublicNeo4jValidationSetApiRegular() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Neo4jApi.EMBEDDED_CORE,
                        Planner.DEFAULT,
                        Runtime.DEFAULT )
        );
    }

    @Test
    void shouldValidateAgainstPublicNeo4jValidationSetApiDense1() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor( CsvSchema.CSV_MERGE, Neo4jSchema.NEO4J_DENSE_1 )
        );
    }

    // TODO un-ignore Cypher validation test. currently disabled to because it takes too long to run.
    @Disabled
    @Test
    void shouldValidateAgainstPublicNeo4jValidationSetCypherDefault() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Neo4jApi.EMBEDDED_CYPHER,
                        Planner.DEFAULT,
                        Runtime.DEFAULT,
                        LdbcDateCodec.Format.NUMBER_UTC,
                        LdbcDateCodec.Format.NUMBER_ENCODED ),
                Planner.DEFAULT,
                Runtime.DEFAULT
        );
    }

    // TODO un-ignore Cypher validation test. currently disabled to because it takes too long to run.
    @Disabled
    @Test
    void shouldValidateAgainstPublicNeo4jValidationSetCypherRemote() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Neo4jApi.REMOTE_CYPHER,
                        Planner.DEFAULT,
                        Runtime.DEFAULT ),
                Planner.DEFAULT,
                Runtime.DEFAULT
        );
    }

    private void doShouldValidateAgainstPublicValidationSet( Scenario scenario ) throws Exception
    {
        doShouldValidateAgainstPublicValidationSet( scenario, Planner.DEFAULT, Runtime.DEFAULT );
    }

    private void doShouldValidateAgainstPublicValidationSet(
            Scenario scenario,
            Planner plannerType,
            Runtime runtimeType ) throws Exception
    {
        assertThat(
                "File is empty: " + scenario.validationParamsFile().getAbsolutePath(),
                scenario.validationParamsFile().length() > 0,
                is( true ) );

        long operationCount = 100000;
        int threadCount = 4;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.directory( "results" ).toFile().toString();
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
                LdbcSnbInteractiveWorkload.class.getName(),
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

        Map<String,String> ldbcSnbInteractiveParameters = DriverConfigUtils.ldbcSnbInteractive();
        ldbcSnbInteractiveParameters.put(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                scenario.paramsDir().getAbsolutePath() );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs( ldbcSnbInteractiveParameters );
        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                MapUtils.loadPropertiesToMap( new File( scenario.updatesDir(), "updateStream.properties" ) )
        );

        /*
        VALIDATE
         */

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

        Store store = Neo4jStore.createFrom( storeDir.toPath() );

        configuration = (ConsoleAndFileDriverConfiguration) configuration.applyArgs(
                Neo4jDb.neo4jConnectorPropertiesFor(
                        scenario.neo4jApi(),
                        scenario.planner(),
                        scenario.runtime(),
                        scenario.neo4jSchema(),
                        store.topLevelDirectory().toFile(),
                        configFile,
                        LdbcSnbInteractiveWorkload.class,
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
        ClientMode<?> clientMode = client.getClientModeFor( controlService );
        clientMode.init();
        DbValidationResult dbValidationResult = ((ValidateDatabaseMode) clientMode).startExecutionAndAwaitCompletion();

        assertThat( dbValidationResult.isSuccessful(), is( true ) );
    }

    private void exportAnonymizedConfiguration(
            DriverConfiguration configuration,
            File validationParameterCreationConfigurationFile ) throws IOException, DriverConfigurationException
    {
        Map<String,String> configurationAsMap = configuration.asMap();
        Map<String,String> anonymizedConfigurationAsMap = new HashMap<>();
        for ( String key : configurationAsMap.keySet() )
        {
            if ( !key.startsWith( "neo4j" ) )
            {
                anonymizedConfigurationAsMap.put( key, configurationAsMap.get( key ) );
            }
        }
        DriverConfiguration anonymizedConfiguration = ConsoleAndFileDriverConfiguration
                .fromParamsMap( anonymizedConfigurationAsMap )
                .applyArg(
                        ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                        "/absolute/path/to/validation_params.csv|500" )
                .applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                        "/absolute/path/to/substitution_parameters" )
                .applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                        "/absolute/path/to/social_network" );

        FileUtils.deleteQuietly( validationParameterCreationConfigurationFile );
        validationParameterCreationConfigurationFile.createNewFile();
        LOG.debug( format(
                "Writing configuration file to: %s",
                validationParameterCreationConfigurationFile.getAbsolutePath()
        ) );
        FileUtils.writeStringToFile(
                validationParameterCreationConfigurationFile,
                anonymizedConfiguration.toPropertiesString(),
                Charset.defaultCharset()
        );
    }
}
