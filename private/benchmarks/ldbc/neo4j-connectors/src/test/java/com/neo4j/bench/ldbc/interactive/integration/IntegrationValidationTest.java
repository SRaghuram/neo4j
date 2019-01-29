/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
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
import com.neo4j.bench.ldbc.DriverConfigUtils;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.LdbcDateCodec;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jImporter;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.LdbcSnbImporter;
import com.neo4j.bench.ldbc.importer.Scenario;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.neo4j.bench.ldbc.DriverConfigUtils.getResource;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IntegrationValidationTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void shouldCreatePublicValidationSet() throws Exception
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
                        Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                        Neo4jApi.EMBEDDED_CORE,
                        PlannerType.DEFAULT,
                        RuntimeType.DEFAULT,
                        LdbcDateCodec.Format.STRING_ENCODED,
                        LdbcDateCodec.Format.NUMBER_UTC,
                        LdbcDateCodec.Resolution.NOT_APPLICABLE
                )
        );
    }

    private void doShouldCreatePublicValidationSet( Scenario scenario ) throws Exception
    {
        File dbDir;

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

        dbDir = temporaryFolder.newFolder();

        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema(),
                scenario.neo4jImporter()
        ).load(
                dbDir,
                scenario.csvDir(),
                DriverConfigUtils.neo4jTestConfig(),
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
                        DriverConfigUtils.neo4jTestConfig(),
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
        ClientMode clientMode = client.getClientModeFor( controlService );
        clientMode.init();
        clientMode.startExecutionAndAwaitCompletion();

        assertThat( scenario.validationParamsFile().length() >= validationSetSize, is( true ) );
    }

    @Test
    public void shouldValidateAgainstPublicNeo4jValidationSetApiRegular() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                        Neo4jApi.EMBEDDED_CORE,
                        PlannerType.DEFAULT,
                        RuntimeType.DEFAULT )
        );
    }

    @Test
    public void shouldValidateAgainstPublicNeo4jValidationSetApiDense1() throws Exception
    {
        // TODO uncomment if/when parallel dense importer is fixed to prevent current issue
//        doShouldValidateAgainstPublicValidationSet(
//                Scenario.randomInteractiveFor( CsvSchema.CSV_MERGE, Neo4jSchema.NEO4J_DENSE_1 )
//        );
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor( CsvSchema.CSV_MERGE, Neo4jSchema.NEO4J_DENSE_1, Neo4jImporter.BATCH )
        );
    }

    // TODO un-ignore Cypher validation test. currently disabled to speed up LDBC automation development.
    @Ignore
    @Test
    public void shouldValidateAgainstPublicNeo4jValidationSetCypherDefault() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                        Neo4jApi.EMBEDDED_CYPHER,
                        PlannerType.DEFAULT,
                        RuntimeType.DEFAULT,
                        LdbcDateCodec.Format.NUMBER_UTC,
                        LdbcDateCodec.Format.NUMBER_ENCODED ),
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT
        );
    }

    // TODO un-ignore Cypher validation test. currently disabled to speed up LDBC automation development.
    @Ignore
    @Test
    public void shouldValidateAgainstPublicNeo4jValidationSetCypherRemote() throws Exception
    {
        doShouldValidateAgainstPublicValidationSet(
                Scenario.randomInteractiveFor(
                        CsvSchema.CSV_REGULAR,
                        Neo4jSchema.NEO4J_REGULAR,
                        Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                        Neo4jApi.REMOTE_CYPHER,
                        PlannerType.DEFAULT,
                        RuntimeType.DEFAULT ),
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT
        );
    }

    private void doShouldValidateAgainstPublicValidationSet( Scenario scenario ) throws Exception
    {
        doShouldValidateAgainstPublicValidationSet( scenario, PlannerType.DEFAULT, RuntimeType.DEFAULT );
    }

    private void doShouldValidateAgainstPublicValidationSet(
            Scenario scenario,
            PlannerType plannerType,
            RuntimeType runtimeType ) throws Exception
    {
        assertThat(
                "File is empty: " + scenario.validationParamsFile().getAbsolutePath(),
                scenario.validationParamsFile().length() > 0,
                is( true ) );

        long operationCount = 100000;
        int threadCount = 4;
        int statusDisplayIntervalAsSeconds = 1;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
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

        File dbDir = temporaryFolder.newFolder();
        LdbcSnbImporter.importerFor(
                scenario.csvSchema(),
                scenario.neo4jSchema(),
                scenario.neo4jImporter()
        ).load(
                dbDir,
                scenario.csvDir(),
                DriverConfigUtils.neo4jTestConfig(),
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
                        DriverConfigUtils.neo4jTestConfig(),
                        LdbcSnbInteractiveWorkload.class,
                        null
                )
        );

        System.out.println( configuration.toPropertiesString() );

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
        System.out.println( format(
                "Writing configuration file to: %s",
                validationParameterCreationConfigurationFile.getAbsolutePath()
        ) );
        FileUtils.writeStringToFile(
                validationParameterCreationConfigurationFile,
                anonymizedConfiguration.toPropertiesString()
        );
    }
}
