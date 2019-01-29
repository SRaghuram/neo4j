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

package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.Client;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.util.FileUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.GraphMetadataProxy;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.profiling.ProfilerRunner;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import com.neo4j.bench.ldbc.utils.Utils;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.neo4j.graphdb.GraphDatabaseService;

import static com.ldbc.driver.control.ConsoleAndFileDriverConfiguration.fromParamsMap;
import static com.ldbc.driver.util.ClassLoaderHelper.loadClass;
import static com.ldbc.driver.util.MapUtils.loadPropertiesToMap;
import static com.neo4j.bench.ldbc.cli.ResultReportingUtil.hasWrites;

import static java.lang.String.format;
import static java.time.Duration.between;
import static java.time.Instant.now;

@Command(
        name = "run",
        description = "Executes an LDBC workload against a Neo4j store" )
public class RunCommand implements Runnable
{
    // ===================================================
    // ================ Tool Configuration ===============
    // ===================================================

    public static final String CMD_LDBC_CONFIG = "--ldbc-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_LDBC_CONFIG},
            description = "LDBC driver configuration file - see:  neo4j-connectors/src/main/resources/ldbc/",
            title = "LDBC Config",
            required = true )
    private File ldbcConfigFile;

    public static final String CMD_WRITES = "--writes";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WRITES},
            description = "Write query parameters directory - see: s3://quality.neotechnology.com/ldbc/csv/",
            title = "Write Parameters",
            required = false )
    private File writeParams;

    public static final String CMD_READS = "--reads";
    @Option( type = OptionType.COMMAND,
            name = {CMD_READS},
            description = "Read query parameters directory - see: s3://quality.neotechnology.com/ldbc/csv/",
            title = "Read Parameters",
            required = false )
    private File readParams;

    public static final String CMD_RESULTS_DIR = "--results";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RESULTS_DIR},
            description = "Benchmark results directory (will be created if does not exist)",
            title = "Results directory",
            required = false )
    private File resultsDir;

    public static final String CMD_READ_THREADS = "--read-threads";
    @Option( type = OptionType.COMMAND,
            name = {CMD_READ_THREADS},
            description = "Number of threads for executing read queries (write thread count is function of dataset)",
            title = "Read thread count",
            required = false )
    private Integer readThreads;

    public static final String CMD_WARMUP_COUNT = "--warmup-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WARMUP_COUNT},
            description = "Number of operations to run during warmup phase",
            title = "Warmup operation count",
            required = false )
    private Long warmupCount;

    public static final String CMD_RUN_COUNT = "--run-count";
    @Option( type = OptionType.COMMAND,
            name = {CMD_RUN_COUNT},
            description = "Number of operations to run during measurement phase",
            title = "Run operation count",
            required = false )
    private Long runCount;

    // ===================================================
    // ================== Neo4j Configuration ============
    // ===================================================

    public static final String CMD_NEO4J_CONFIG = "--neo4j-config";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_CONFIG},
            description = "Neo4j configuration file - see:  neo4j-connectors/src/main/resources/neo4j/",
            title = "Neo4j Config",
            required = false )
    private File neo4jConfig;

    public static final String CMD_DB = "--db";
    @Option( type = OptionType.COMMAND,
            name = {CMD_DB},
            description = "Target Neo4j database directory - see: s3://quality.neotechnology.com/ldbc/db/",
            title = "Database Directory",
            required = false )
    private File neo4jDir;

    public static final String CMD_NEO4J_API = "--neo4j-api";
    @Option( type = OptionType.COMMAND,
            name = {CMD_NEO4J_API},
            description = "Neo4j surface API: EMBEDDED_CORE, EMBEDDED_CYPHER, REMOTE_CYPHER",
            title = "Neo4j API",
            required = false )
    private Neo4jApi neo4jApi;

    public static final String CMD_CYPHER_PLANNER = "--planner";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CYPHER_PLANNER},
            description = "Cypher Planner: DEFAULT, RULE, COST",
            title = "Cypher Planner",
            required = false )
    private PlannerType planner = PlannerType.DEFAULT;

    public static final String CMD_CYPHER_RUNTIME = "--runtime";
    @Option( type = OptionType.COMMAND,
            name = {CMD_CYPHER_RUNTIME},
            description = "Cypher Runtime",
            title = "Cypher Runtime: DEFAULT, INTERPRETED, COMPILED",
            required = false )
    private RuntimeType runtime = RuntimeType.DEFAULT;

    public static final String CMD_WAIT_FOR_FILE = "--wait-for-file";
    @Option( type = OptionType.COMMAND,
            name = {CMD_WAIT_FOR_FILE},
            description = "If set, process will not exit until it finds this file",
            title = "Wait on file",
            required = false )
    private File waitForFile;

    @Override
    public void run()
    {
        System.out.println( format( "Neo4j Directory             : %s",
                                    (null == neo4jDir) ? null : neo4jDir.getAbsolutePath() ) );
        System.out.println( format( "Write Queries Directory     : %s",
                                    (null == writeParams) ? null : writeParams.getAbsolutePath() ) );
        System.out.println( format( "Read Queries Directory      : %s",
                                    (null == readParams) ? null : readParams.getAbsolutePath() ) );
        System.out.println( format( "Results Directory           : %s",
                                    (null == resultsDir) ? null : resultsDir.getAbsolutePath() ) );
        System.out.println( format( "Warmup Count                : %s", warmupCount ) );
        System.out.println( format( "Run Count                   : %s", runCount ) );
        System.out.println( format( "Neo4j API                   : %s", neo4jApi ) );
        System.out.println( format( "Cypher Planner              : %s", planner ) );
        System.out.println( format( "Cypher Runtime              : %s", runtime ) );
        System.out.println( format( "LDBC Configuration          : %s",
                                    (null == ldbcConfigFile) ? null : ldbcConfigFile.getAbsolutePath() ) );
        System.out.println( format( "Neo4j Configuration         : %s",
                                    (null == neo4jConfig) ? null : neo4jConfig.getAbsolutePath() ) );
        System.out.println( format( "Read Threads                : %s", readThreads ) );

        try
        {
            DriverConfiguration ldbcConfig = fromParamsMap( loadPropertiesToMap( ldbcConfigFile ) );

            String neo4jConnector = discoverConnector( neo4jDir, neo4jConfig, neo4jApi, ldbcConfigFile );
            if ( null == neo4jConnector )
            {
                throw new RuntimeException( "Parameter not set: " + CMD_NEO4J_API );
            }
            System.out.println( format( "Neo4j Connector (inferred)  : %s", neo4jConnector ) );

            if ( null != writeParams )
            {
                FileUtils.assertDirectoryExists( writeParams );
            }
            readParams = getFileArgOrFail(
                    readParams,
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    ldbcConfig );
            resultsDir = getFileArgOrFail(
                    resultsDir,
                    ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                    ldbcConfig );
            FileUtils.assertDirectoryExists( readParams );
            if ( !neo4jConnector.equals( Neo4jDb.DB_TYPE_VALUE__REMOTE_CYPHER ) )
            {
                FileUtils.assertDirectoryExists( neo4jDir );
            }

            if ( null != writeParams &&
                 LdbcSnbInteractiveWorkload.class.getName().equals( ldbcConfig.workloadClassName() ) &&
                 hasWrites( ldbcConfig ) )
            {
                File writeParamsConfig = new File( writeParams, "updateStream.properties" );
                FileUtils.assertFileExists( writeParamsConfig );
                System.out.println( format( "Write Threads (inferred)    : %s",
                                            LdbcSnbInteractiveWorkloadConfiguration.forumUpdateFilesInDirectory( writeParams ).size() ) );
                ldbcConfig = ldbcConfig.applyArgs( loadPropertiesToMap( writeParamsConfig ) );
            }

            System.out.println( "*** Neo4j DB Properties ***" );
            try
            {
                System.out.println( Neo4jDb.configToString( neo4jConfig ) );
            }
            catch ( DbException e )
            {
                throw new RuntimeException( "Unable to read Neo4j configuration file to string", e );
            }
            System.out.println( "************************" );

            if ( null != readThreads )
            {
                ldbcConfig = ldbcConfig.applyArg(
                        ConsoleAndFileDriverConfiguration.THREADS_ARG, Integer.toString( readThreads ) );
            }
            ldbcConfig = ldbcConfig.applyArg(
                    ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG, resultsDir.getAbsolutePath() );
            if ( !neo4jConnector.equals( Neo4jDb.DB_TYPE_VALUE__REMOTE_CYPHER ) )
            {
                ldbcConfig = ldbcConfig.applyArg( Neo4jDb.DB_PATH_KEY, neo4jDir.getAbsolutePath() );
            }
            ldbcConfig = ldbcConfig.applyArg( Neo4jDb.DB_TYPE_KEY, neo4jConnector );
            if ( !neo4jConnector.equals( Neo4jDb.DB_TYPE_VALUE__REMOTE_CYPHER ) )
            {
                ldbcConfig = ldbcConfig.applyArg( Neo4jDb.CONFIG_PATH_KEY, neo4jConfig.getAbsolutePath() );
            }
            if ( null != writeParams )
            {
                ldbcConfig = ldbcConfig.applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY, writeParams.getAbsolutePath() );
            }
            ldbcConfig = ldbcConfig.applyArg(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY, readParams.getAbsolutePath() );
            if ( null != warmupCount )
            {
                ldbcConfig = ldbcConfig.applyArg(
                        ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( warmupCount ) );
            }
            if ( null != runCount )
            {
                ldbcConfig = ldbcConfig.applyArg(
                        ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( runCount ) );
            }
            if ( null != planner )
            {
                ldbcConfig = ldbcConfig.applyArg( Neo4jDb.CYPHER_PLANNER_KEY, planner.name() );
            }
            if ( null != runtime )
            {
                ldbcConfig = ldbcConfig.applyArg( Neo4jDb.CYPHER_RUNTIME_KEY, runtime.name() );
            }
            Client.main( ((ConsoleAndFileDriverConfiguration) ldbcConfig).toArgs() );

            if ( null != waitForFile )
            {
                Instant waitForFileStart = Instant.now();
                Duration waitForFileTimeout = Duration.of( 10, ChronoUnit.MINUTES );
                while ( !waitForFile.exists() )
                {
                    System.out.println( "Fork waiting on parent to finish profiling. Has waited: " + between( waitForFileStart, now() ) );
                    Thread.sleep( 5000 );
                    ProfilerRunner.checkTimeout( waitForFileStart, waitForFileTimeout );
                }
                FileUtils.assertFileExists( waitForFile );
            }
            System.out.println( "Forked process complete!" );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            System.exit( 1 );
        }
    }

    static File getFileArgOrFail( File maybeFile, String arg, DriverConfiguration ldbcConfig )
    {
        if ( null == maybeFile )
        {
            if ( ldbcConfig.asMap().containsKey( arg ) )
            {
                return new File( ldbcConfig.asMap().get( arg ) );
            }
            else
            {
                throw new RuntimeException( "Missing argument: " + arg );
            }
        }
        else
        {
            return maybeFile;
        }
    }

    static String[] buildArgs( LdbcRunConfig ldbcRunConfig, File resultsDir )
    {
        String[] args = new String[]{
                "run",
                CMD_READS, ldbcRunConfig.readParams.getAbsolutePath(),
                CMD_RESULTS_DIR, resultsDir.getAbsolutePath(),
                CMD_NEO4J_API, ldbcRunConfig.neo4jApi.name(),
                CMD_CYPHER_PLANNER, ldbcRunConfig.planner.name(),
                CMD_CYPHER_RUNTIME, ldbcRunConfig.runtime.name(),
                CMD_LDBC_CONFIG, ldbcRunConfig.ldbcConfig.getAbsolutePath(),
                CMD_READ_THREADS, Integer.toString( ldbcRunConfig.readThreads )
        };
        if ( !ldbcRunConfig.neo4jApi.isRemote() )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_DB );
            args = Utils.copyArrayAndAddElement( args, ldbcRunConfig.dbDir.getAbsolutePath() );
            args = Utils.copyArrayAndAddElement( args, CMD_NEO4J_CONFIG );
            args = Utils.copyArrayAndAddElement( args, ldbcRunConfig.neo4jConfig.getAbsolutePath() );
        }
        if ( null != ldbcRunConfig.writeParams )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WRITES );
            args = Utils.copyArrayAndAddElement( args, ldbcRunConfig.writeParams.getAbsolutePath() );
        }
        if ( null != ldbcRunConfig.warmupCount )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WARMUP_COUNT );
            args = Utils.copyArrayAndAddElement( args, Long.toString( ldbcRunConfig.warmupCount ) );
        }
        if ( null != ldbcRunConfig.runCount )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_RUN_COUNT );
            args = Utils.copyArrayAndAddElement( args, Long.toString( ldbcRunConfig.runCount ) );
        }
        if ( null != ldbcRunConfig.waitForFile )
        {
            args = Utils.copyArrayAndAddElement( args, CMD_WAIT_FOR_FILE );
            args = Utils.copyArrayAndAddElement( args, ldbcRunConfig.waitForFile.getAbsolutePath() );
        }
        return args;
    }

    private static String discoverConnector( File neo4jDir, File neo4jConfig, Neo4jApi neo4jApi, File ldbcConfig )
    {
        try
        {
            DriverConfiguration config = fromParamsMap( loadPropertiesToMap( ldbcConfig ) );
            if ( null == neo4jApi )
            {
                return config.asMap().get( Neo4jDb.DB_TYPE_KEY );
            }
            else
            {
                Class<? extends Workload> workload = loadClass( config.workloadClassName(), Workload.class );
                Neo4jSchema neo4jSchema = discoverSchema( neo4jDir, neo4jConfig, neo4jApi );
                return Neo4jDb.neo4jConnectorFor( neo4jApi, neo4jSchema, workload );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error discovering Neo4j connector to use", e );
        }
    }

    static Neo4jSchema discoverSchema( File neo4jDir, File neo4jConfig, Neo4jApi neo4jApi )
    {
        if ( null != neo4jApi && neo4jApi.isRemote() )
        {
            // Fall back to regular schema
            // This error can happen if another process already started the DB, which is common in 'remote' scenario
            return Neo4jSchema.NEO4J_REGULAR;
        }
        GraphDatabaseService db = Neo4jDb.newDb( neo4jDir, neo4jConfig );
        try
        {
            GraphMetadataProxy metadataProxy = GraphMetadataProxy.loadFrom( db );
            db.shutdown();
            return metadataProxy.neo4jSchema();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error inspecting database schema", e );
        }
        finally
        {
            db.shutdown();
        }
    }

    static class LdbcRunConfig
    {
        final File dbDir;
        final File writeParams;
        final File readParams;
        final Neo4jApi neo4jApi;
        final PlannerType planner;
        final RuntimeType runtime;
        final File ldbcConfig;
        final File neo4jConfig;
        final int readThreads;
        final Long warmupCount;
        final Long runCount;
        final File waitForFile;

        LdbcRunConfig(
                File dbDir,
                File writeParams,
                File readParams,
                Neo4jApi neo4jApi,
                PlannerType planner,
                RuntimeType runtime,
                File ldbcConfig,
                File neo4jConfig,
                int readThreads,
                Long warmupCount,
                Long runCount,
                File waitForFile )
        {
            this.dbDir = dbDir;
            this.writeParams = writeParams;
            this.readParams = readParams;
            this.neo4jApi = neo4jApi;
            this.planner = planner;
            this.runtime = runtime;
            this.ldbcConfig = ldbcConfig;
            this.neo4jConfig = neo4jConfig;
            this.readThreads = readThreads;
            this.warmupCount = warmupCount;
            this.runCount = runCount;
            this.waitForFile = waitForFile;
        }
    }
}
