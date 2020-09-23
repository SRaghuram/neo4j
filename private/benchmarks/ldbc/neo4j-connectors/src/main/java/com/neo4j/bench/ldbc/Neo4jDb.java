/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiCypherQueries;
import com.neo4j.bench.ldbc.business_intelligence.SnbBiEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveCypherQueries;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCoreDense1Commands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCoreRegularCommands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveEmbeddedCypherRegularCommands;
import com.neo4j.bench.ldbc.interactive.SnbInteractiveRemoteCypherRegularCommands;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.io.layout.DatabaseLayout;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

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
            commands = new SnbInteractiveEmbeddedCypherRegularCommands(
                    dbDir,
                    configFile,
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
            commands = new SnbBiEmbeddedCypherRegularCommands(
                    dbDir,
                    configFile,
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

    private Planner getCypherPlannerOrFail( String cypherPlannerString ) throws DbException
    {
        if ( null == cypherPlannerString )
        {
            throw new DbException( format( "No value provided for %s, expected one of: %s",
                                           CYPHER_PLANNER_KEY, Arrays.toString( Planner.values() ) ) );
        }
        else
        {
            try
            {
                return Planner.valueOf( cypherPlannerString.trim().toUpperCase() );
            }
            catch ( Exception e )
            {
                throw new DbException(
                        format( "Unknown value provided for %s: %s\nValid values are: %s",
                                CYPHER_PLANNER_KEY,
                                cypherPlannerString,
                                Arrays.toString( Planner.values() ) ),
                        e
                );
            }
        }
    }

    private Runtime getCypherRuntimeOrFail( String cypherRuntimeString ) throws DbException
    {
        if ( null == cypherRuntimeString )
        {
            throw new DbException( format( "No value provided for %s, expected one of: %s",
                                           CYPHER_RUNTIME_KEY, Arrays.toString( Runtime.values() ) ) );
        }
        else
        {
            try
            {
                return Runtime.valueOf( cypherRuntimeString.trim().toUpperCase() );
            }
            catch ( Exception e )
            {
                throw new DbException(
                        format( "Unknown cypher runtime: %s\nValid values are: %s",
                                cypherRuntimeString,
                                Arrays.toString( Runtime.values() ) ),
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

    public static DatabaseLayout layoutWithTxLogLocation( File homeDir )
    {
        return DatabaseLayout.of( Config.defaults( neo4j_home, homeDir.toPath().toAbsolutePath() ) );
    }

    public static DatabaseManagementService newDb( File homeDir, File configFile )
    {
        return newDbBuilder( homeDir, configFile ).build();
    }

    private static DatabaseManagementServiceBuilder newDbBuilder( File homeDir, File configFile )
    {
        DatabaseManagementServiceBuilder builder = new EnterpriseDatabaseManagementServiceBuilder( homeDir.toPath() );
        if ( null != configFile )
        {
            builder = builder.loadPropertiesFromFile( configFile.toPath() );
        }
        return builder;
    }

    public static DatabaseManagementServiceBuilder newDbBuilderForBolt( File homeDir, File configFile, URI uri )
    {
        String withoutProtocol = uri.toString().substring( uri.toString().indexOf( "://" ) + 3 );
        int portIndex = withoutProtocol.lastIndexOf( ':' );
        int port = Integer.parseInt( withoutProtocol.substring( portIndex + 1 ) );
        return newDbBuilderForBolt(
                homeDir,
                configFile,
                withoutProtocol.substring( 0, portIndex ),
                port );
    }

    public static DatabaseManagementServiceBuilder newDbBuilderForBolt( File homeDir, File configFile, String uriString, int port )
    {
        return newDbBuilder( homeDir, configFile )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED )
                .setConfig( BoltConnector.listen_address, new SocketAddress( uriString, port ) );
    }

    public static String configToString( File configFile )
    {
        if ( null == configFile )
        {
            return "!! No file provided (was null) !!";
        }
        else
        {
            return BenchmarkUtil.fileToString( configFile.toPath() );
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
                    format( "Unsupported workload: %s", workloadClass.getSimpleName() ) );
        }
    }

    public static Map<String,String> neo4jConnectorPropertiesFor(
            Neo4jApi neo4jApi,
            Planner planner,
            Runtime runtime,
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
                    format( "Unsupported workload: %s", workloadClass.getSimpleName() ) );
        }
    }
}
