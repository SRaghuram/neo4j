/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiCypherQueries;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveCypherQueries;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCoreDense1Commands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCoreRegularCommands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveRemoteCypherRegularCommands;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector;
import org.neo4j.kernel.configuration.BoltConnector;

import static java.lang.String.format;

public class Neo4jDb extends Db
{
    public static final String DB_PATH_KEY = "neo4j.path";
    public static final String CONFIG_PATH_KEY = "neo4j.config";
    public static final String URL_KEY = "neo4j.url";
    public static final String USERNAME_KEY = "neo4j.username";
    public static final String PASSWORD_KEY = "neo4j.password";
    // TODO replace dbtype with:
    // TODO * connector: remote/embedded
    // TODO * api: cypher/core
    // TODO * neo4jSchema: regular/dense1
    // TODO * workload: ALREADY AVAILABLE FROM PARAMS
    public static final String DB_TYPE_KEY = "neo4j.dbtype";
    public static final String DB_TYPE_VALUE__REMOTE_CYPHER = "remote-cypher";
    public static final String DB_TYPE_VALUE__EMBEDDED_CYPHER = "embedded-cypher";
    public static final String DB_TYPE_VALUE__EMBEDDED_API = "embedded-api";
    public static final String DB_TYPE_VALUE__EMBEDDED_API_DENSE_1 = "embedded-api-dense-1";
    public static final String DB_TYPE_VALUE__BI_EMBEDDED_CYPHER = "bi-embedded-cypher";

    public static final String DO_WARMUP_KEY = "neo4j.do_warmup";
    private static final boolean DEFAULT_DO_WARMUP_VALUE = false;

    public static final String CYPHER_PLANNER_KEY = "neo4j.planner";
    public static final String CYPHER_RUNTIME_KEY = "neo4j.runtime";

    private Neo4jDbCommands commands;

    @Override
    protected void onInit( Map<String,String> params, LoggingService loggingService ) throws DbException
    {
        // Initialize Neo4j driver
        String uriString = params.get( URL_KEY );
        String username = params.get( USERNAME_KEY );
        String password = params.get( PASSWORD_KEY );
        String dbPath = params.get( DB_PATH_KEY );
        String configPath = params.get( CONFIG_PATH_KEY );
        String dbType = params.get( DB_TYPE_KEY );
        String cypherPlannerString = params.get( CYPHER_PLANNER_KEY );
        String cypherRuntimeString = params.get( CYPHER_RUNTIME_KEY );
        String doWarmupString = params.get( DO_WARMUP_KEY );

        File dbDir = (null == dbPath) ? null : new File( dbPath );
        File configFile = (null == configPath) ? null : new File( configPath );
        boolean doWarmup = assertAndParseWarmupString( doWarmupString, DEFAULT_DO_WARMUP_VALUE );

        if ( null == dbType )
        {
            throw new DbException( "Missing configuration settings: " + DB_TYPE_KEY );
        }

        switch ( dbType )
        {
        case DB_TYPE_VALUE__EMBEDDED_CYPHER:
        {
            printConfigCypher(
                    dbType,
                    dbDir,
                    configFile,
                    cypherPlannerString,
                    cypherRuntimeString,
                    doWarmup,
                    loggingService );
            assertValidDbDir( dbDir );
            assertValidConfigFile( configFile );
            loggingService.info( "Connecting to database: " + dbDir.getAbsolutePath() );
            String resultDirPath = params.get( ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG );
            File resultDir = new File( resultDirPath );
            String benchmarkName = params.get( ConsoleAndFileDriverConfiguration.NAME_ARG );
            commands = new SnbInteractiveEmbeddedCypherRegularCommands(
                    dbDir,
                    configFile,
                    resultDir,
                    benchmarkName,
                    loggingService,
                    SnbInteractiveCypherQueries.createWith(
                            getCypherPlannerOrFail( cypherPlannerString ),
                            getCypherRuntimeOrFail( cypherRuntimeString )
                                                          ),
                    doWarmup
            );
            break;
        }
        case DB_TYPE_VALUE__EMBEDDED_API:
        {
            printConfigCore(
                    dbType,
                    dbDir,
                    configFile,
                    doWarmup,
                    loggingService );
            assertValidDbDir( dbDir );
            assertValidConfigFile( configFile );
            loggingService.info( "Connecting to database: " + dbDir.getAbsolutePath() );
            commands = new SnbInteractiveEmbeddedCoreRegularCommands(
                    dbDir,
                    configFile,
                    loggingService,
                    doWarmup
            );
            break;
        }
        case DB_TYPE_VALUE__EMBEDDED_API_DENSE_1:
        {
            printConfigCore(
                    dbType,
                    dbDir,
                    configFile,
                    doWarmup,
                    loggingService );
            assertValidDbDir( dbDir );
            assertValidConfigFile( configFile );
            loggingService.info( "Connecting to database: " + dbDir.getAbsolutePath() );
            commands = new SnbInteractiveEmbeddedCoreDense1Commands(
                    dbDir,
                    configFile,
                    loggingService,
                    doWarmup
            );
            break;
        }
        case DB_TYPE_VALUE__REMOTE_CYPHER:
        {
            URI uri = (null == uriString) ? null : URI.create( uriString );
            AuthToken authToken;
            if ( null == username )
            {
                authToken = AuthTokens.none();
            }
            else if ( null == password )
            {
                throw new IllegalStateException( "No password specified for username: " + username );
            }
            else
            {
                authToken = AuthTokens.basic( username, password );
            }
            if ( null != uri )
            {
                printConfigCypher(
                        dbType,
                        dbDir,
                        configFile,
                        cypherPlannerString,
                        cypherRuntimeString,
                        doWarmup,
                        loggingService );
                assertValidDbUri( uri );
                commands = new SnbInteractiveRemoteCypherRegularCommands(
                        uri,
                        authToken,
                        loggingService,
                        SnbInteractiveCypherQueries.createWith(
                                getCypherPlannerOrFail( cypherPlannerString ),
                                getCypherRuntimeOrFail( cypherRuntimeString )
                                                              ),
                        null,
                        null
                );
                loggingService.info( "Connecting to database: " + uri.toString() );
            }
            else
            {
                assertValidDbDir( dbDir );
                assertValidConfigFile( configFile );
                commands = new SnbInteractiveRemoteCypherRegularCommands(
                        null,
                        null,
                        loggingService,
                        SnbInteractiveCypherQueries.createWith(
                                getCypherPlannerOrFail( cypherPlannerString ),
                                getCypherRuntimeOrFail( cypherRuntimeString )
                                                              ),
                        dbDir,
                        configFile
                );
                loggingService.info( "Connecting to database: " + dbDir.getAbsolutePath() );
            }
            break;
        }
        case DB_TYPE_VALUE__BI_EMBEDDED_CYPHER:
        {
            printConfigCypher(
                    dbType,
                    dbDir,
                    configFile,
                    cypherPlannerString,
                    cypherRuntimeString,
                    doWarmup,
                    loggingService );
            assertValidDbDir( dbDir );
            assertValidConfigFile( configFile );
            loggingService.info( "Connecting to database: " + dbDir.getAbsolutePath() );

            String resultDirPath = params.get( ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG );
            File resultDir = new File( resultDirPath );
            String benchmarkName = params.get( ConsoleAndFileDriverConfiguration.NAME_ARG );
            commands = new SnbBiEmbeddedCypherRegularCommands(
                    dbDir,
                    configFile,
                    resultDir,
                    benchmarkName,
                    loggingService,
                    SnbBiCypherQueries.createWith(
                            getCypherPlannerOrFail( cypherPlannerString ),
                            getCypherRuntimeOrFail( cypherRuntimeString )
                                                 ),
                    doWarmup
            );
            break;
        }
        default:
            throw new DbException( format( "Invalid database type: %s", dbType ) );
        }

        commands.init();
        commands.registerHandlersWithDb( this );
        loggingService.info( "Initialization complete" );
    }

    @Override
    protected void onClose() throws IOException
    {
        commands.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException
    {
        return commands.getConnectionState();
    }

    private void printConfigCore(
            String dbType,
            File dbDir,
            File configFile,
            boolean doWarmup,
            LoggingService loggingService ) throws DbException
    {
        loggingService.info( "*** Neo4j Connector Properties ***" );
        loggingService.info( "database type = " + dbType );
        loggingService.info( "db path = " + ((null == dbDir) ? "UNKNOWN" : dbDir.getAbsolutePath()) );
        loggingService.info( "config path = " + ((null == configFile) ? "UNKNOWN" : configFile.getAbsolutePath()) );
        loggingService.info( "do warmup = " + doWarmup );
        loggingService.info( "*** Neo4j DB Properties ***" );
        loggingService.info( configToString( configFile ) );
        loggingService.info( "************************" );
    }

    private void printConfigCypher(
            String dbType,
            File dbDir,
            File configFile,
            String cypherPlannerString,
            String cypherRuntimeString,
            boolean doWarmup,
            LoggingService loggingService ) throws DbException
    {
        loggingService.info( "*** Neo4j Connector Properties ***" );
        loggingService.info( "database type = " + dbType );
        loggingService.info( "cypher planner = " + ((null == cypherPlannerString) ? "UNKNOWN" : cypherPlannerString) );
        loggingService.info( "cypher runtime = " + ((null == cypherRuntimeString) ? "UNKNOWN" : cypherRuntimeString) );
        loggingService.info( "db path = " + ((null == dbDir) ? "UNKNOWN" : dbDir.getAbsolutePath()) );
        loggingService.info( "config path = " + ((null == configFile) ? "UNKNOWN" : configFile.getAbsolutePath()) );
        loggingService.info( "do warmup = " + doWarmup );
        loggingService.info( "*** Neo4j DB Properties ***" );
        loggingService.info( configToString( configFile ) );
        loggingService.info( "************************" );
    }

    private PlannerType getCypherPlannerOrFail( String cypherPlannerString ) throws DbException
    {
        if ( null == cypherPlannerString )
        {
            throw new DbException( format( "No value provided for %s, expected one of: %s",
                                           CYPHER_PLANNER_KEY, Arrays.toString( PlannerType.values() ) ) );
        }
        else
        {
            try
            {
                return PlannerType.valueOf( cypherPlannerString.trim().toUpperCase() );
            }
            catch ( Exception e )
            {
                throw new DbException(
                        format( "Unknown value provided for %s: %s\nValid values are: %s",
                                CYPHER_PLANNER_KEY,
                                cypherPlannerString,
                                Arrays.toString( PlannerType.values() ) ),
                        e
                );
            }
        }
    }

    private RuntimeType getCypherRuntimeOrFail( String cypherRuntimeString ) throws DbException
    {
        if ( null == cypherRuntimeString )
        {
            throw new DbException( format( "No value provided for %s, expected one of: %s",
                                           CYPHER_RUNTIME_KEY, Arrays.toString( RuntimeType.values() ) ) );
        }
        else
        {
            try
            {
                return RuntimeType.valueOf( cypherRuntimeString.trim().toUpperCase() );
            }
            catch ( Exception e )
            {
                throw new DbException(
                        format( "Unknown cypher runtime: %s\nValid values are: %s",
                                cypherRuntimeString,
                                Arrays.toString( RuntimeType.values() ) ),
                        e
                );
            }
        }
    }

    private void assertValidDbDir( File dbDir ) throws DbException
    {
        if ( null == dbDir )
        {
            throw new DbException( "Neo4j path not given" );
        }
        else if ( !dbDir.exists() )
        {
            throw new DbException( format( "Neo4j path does not exist: %s", dbDir.getAbsolutePath() ) );
        }
        else if ( !dbDir.isDirectory() )
        {
            throw new DbException( format( "Neo4j path is not a directory: %s", dbDir.getAbsolutePath() ) );
        }
    }

    private void assertValidConfigFile( File configFile ) throws DbException
    {
        if ( null != configFile && !configFile.exists() )
        {
            throw new DbException( "Neo4j configuration file does not exist: " + configFile.getAbsolutePath() );
        }
    }

    private void assertValidDbUri( URI uri ) throws DbException
    {
        if ( null == uri )
        {
            throw new DbException( "Neo4j server URI not given" );
        }
    }

    private boolean assertAndParseWarmupString( String doWarmupString, boolean defaultValue ) throws DbException
    {
        if ( null == doWarmupString )
        {
            return defaultValue;
        }
        else if ( doWarmupString.toLowerCase().equals( "false" ) || doWarmupString.toLowerCase().equals( "true" ) )
        {
            return Boolean.parseBoolean( doWarmupString );
        }
        else
        {
            throw new DbException( format( "Invalid %s value: %s", DO_WARMUP_KEY, doWarmupString ) );
        }
    }

    // ================================================================================================================
    // ==========================================  UTILS  =============================================================
    // ================================================================================================================

    public static GraphDatabaseService newDb( File dbDir, File configFile )
    {
        return newDbBuilder( dbDir, configFile ).newGraphDatabase();
    }

    private static GraphDatabaseBuilder newDbBuilder( File dbDir, File configFile )
    {
        GraphDatabaseBuilder builder = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( dbDir );
        if ( null != configFile )
        {
            builder = builder.loadPropertiesFromFile( configFile.getAbsolutePath() );
        }
        return builder;
    }

    public static GraphDatabaseBuilder newDbBuilderForBolt( File dbDir, File configFile, URI uri )
    {
        String withoutProtocol = uri.toString().substring(
                uri.toString().indexOf( "://" ) + 3,
                uri.toString().length() );
        int portIndex = withoutProtocol.lastIndexOf( ":" );
        int port = Integer.parseInt( withoutProtocol.substring( portIndex + 1, withoutProtocol.length() ) );
        return newDbBuilderForBolt(
                dbDir,
                configFile,
                withoutProtocol.substring( 0, portIndex ),
                port );
    }

    public static GraphDatabaseBuilder newDbBuilderForBolt( File dbDir, File configFile, String uriString, int port )
    {
        return newDbBuilder( dbDir, configFile )
                .setConfig( Neo4jDb.boltConnector().enabled, "true" )
                .setConfig( Neo4jDb.boltConnector().type, Connector.ConnectorType.BOLT.name() )
                .setConfig( Neo4jDb.boltConnector().encryption_level, BoltConnector.EncryptionLevel.DISABLED.name() )
                .setConfig( Neo4jDb.boltConnector().listen_address, uriString + ":" + port );
    }

    private static BoltConnector boltConnector()
    {
        return new BoltConnector( "bolt" );
    }

    public static String configToString( File configFile ) throws DbException
    {
        if ( null == configFile )
        {
            return "!! No file provided (was null) !!";
        }
        else
        {
            try
            {
                return FileUtils.readFileToString( configFile );
            }
            catch ( IOException e )
            {
                throw new DbException( "Error reading Neo4j configuration contents to string", e );
            }
        }
    }

    public static String neo4jConnectorFor(
            Neo4jApi neo4jApi,
            Neo4jSchema neo4jSchema,
            Class<? extends Workload> workloadClass )
    {
        if ( workloadClass.equals( LdbcSnbBiWorkload.class ) )
        {
            switch ( neo4jApi )
            {
            case EMBEDDED_CYPHER:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    return DB_TYPE_VALUE__BI_EMBEDDED_CYPHER;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            default:
                throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                    neo4jApi,
                                                    neo4jSchema,
                                                    workloadClass.getSimpleName() ) );
            }
        }
        else if ( workloadClass.equals( LdbcSnbInteractiveWorkload.class ) )
        {
            switch ( neo4jApi )
            {
            case EMBEDDED_CORE:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    return DB_TYPE_VALUE__EMBEDDED_API;
                }
                case NEO4J_DENSE_1:
                {
                    return DB_TYPE_VALUE__EMBEDDED_API_DENSE_1;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            case EMBEDDED_CYPHER:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    return DB_TYPE_VALUE__EMBEDDED_CYPHER;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            case REMOTE_CYPHER:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    return Neo4jDb.DB_TYPE_VALUE__REMOTE_CYPHER;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            default:
                throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                    neo4jApi,
                                                    neo4jSchema,
                                                    workloadClass.getSimpleName() ) );
            }
        }
        else
        {
            throw new RuntimeException(
                    format( "Unsupported workload: %s", workloadClass.getClass().getSimpleName() ) );
        }
    }

    public static Map<String,String> neo4jConnectorPropertiesFor(
            Neo4jApi neo4jApi,
            PlannerType planner,
            RuntimeType runtime,
            Neo4jSchema neo4jSchema,
            File dbDir,
            File dbConfig,
            Class<? extends Workload> workloadClass,
            String url )
    {
        if ( workloadClass.equals( LdbcSnbBiWorkload.class ) )
        {
            switch ( neo4jApi )
            {
            case EMBEDDED_CYPHER:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    Map<String,String> params = new HashMap<>();
                    params.put( DB_TYPE_KEY, DB_TYPE_VALUE__BI_EMBEDDED_CYPHER );
                    params.put( CYPHER_PLANNER_KEY, planner.name() );
                    params.put( CYPHER_RUNTIME_KEY, runtime.name() );
                    params.put( DB_PATH_KEY, dbDir.getAbsolutePath() );
                    params.put( CONFIG_PATH_KEY, dbConfig.getAbsolutePath() );
                    return params;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            default:
                throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                    neo4jApi,
                                                    neo4jSchema,
                                                    workloadClass.getSimpleName() ) );
            }
        }
        else if ( workloadClass.equals( LdbcSnbInteractiveWorkload.class ) )
        {
            switch ( neo4jApi )
            {
            case EMBEDDED_CORE:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    Map<String,String> params = new HashMap<>();
                    params.put( DB_TYPE_KEY, DB_TYPE_VALUE__EMBEDDED_API );
                    params.put( CYPHER_PLANNER_KEY, planner.name() );
                    params.put( CYPHER_RUNTIME_KEY, runtime.name() );
                    params.put( DB_PATH_KEY, dbDir.getAbsolutePath() );
                    params.put( CONFIG_PATH_KEY, dbConfig.getAbsolutePath() );
                    return params;
                }
                case NEO4J_DENSE_1:
                {
                    Map<String,String> params = new HashMap<>();
                    params.put( DB_TYPE_KEY, DB_TYPE_VALUE__EMBEDDED_API_DENSE_1 );
                    params.put( DB_PATH_KEY, dbDir.getAbsolutePath() );
                    params.put( CONFIG_PATH_KEY, dbConfig.getAbsolutePath() );
                    return params;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            case EMBEDDED_CYPHER:
                switch ( neo4jSchema )
                {
                case NEO4J_REGULAR:
                {
                    Map<String,String> params = new HashMap<>();
                    params.put( DB_TYPE_KEY, DB_TYPE_VALUE__EMBEDDED_CYPHER );
                    params.put( CYPHER_PLANNER_KEY, planner.name() );
                    params.put( CYPHER_RUNTIME_KEY, runtime.name() );
                    params.put( DB_PATH_KEY, dbDir.getAbsolutePath() );
                    params.put( CONFIG_PATH_KEY, dbConfig.getAbsolutePath() );
                    return params;
                }
                default:
                    throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                        neo4jApi,
                                                        neo4jSchema,
                                                        workloadClass.getSimpleName() ) );
                }
            case REMOTE_CYPHER:
            {
                Map<String,String> params = new HashMap<>();
                params.put( Neo4jDb.DB_TYPE_KEY, Neo4jDb.DB_TYPE_VALUE__REMOTE_CYPHER );
                params.put( CYPHER_PLANNER_KEY, planner.name() );
                params.put( CYPHER_RUNTIME_KEY, runtime.name() );
                if ( null == url )
                {
                    params.put( Neo4jDb.DB_PATH_KEY, dbDir.getAbsolutePath() );
                }
                else
                {
                    params.put( Neo4jDb.URL_KEY, url );
                }
                params.put( Neo4jDb.CONFIG_PATH_KEY, dbConfig.getAbsolutePath() );
                return params;
            }
            default:
                throw new RuntimeException( format( "Unsupported combination: %s / %s / %s",
                                                    neo4jApi,
                                                    neo4jSchema,
                                                    workloadClass.getSimpleName() ) );
            }
        }
        else
        {
            throw new RuntimeException(
                    format( "Unsupported workload: %s", workloadClass.getClass().getSimpleName() ) );
        }
    }
}
