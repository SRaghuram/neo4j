/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.DatabaseName;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.Neo4jServerWrapper.Neo4jServerConnection;
import com.neo4j.bench.model.model.PlanOperator;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static java.lang.ProcessBuilder.Redirect;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

public class ServerDatabase implements Database
{
    public static ServerDatabase startServer( Jvm jvm,
                                              Path neo4jDir,
                                              Store store,
                                              Path neo4jConfigFile,
                                              Redirect outputRedirect,
                                              Redirect errorRedirect,
                                              Path copyLogsToOnClose,
                                              int port )
    {
        DatabaseName databaseName = store.databaseName();

        Neo4jConfigBuilder.fromFile( neo4jConfigFile )
                          .withSetting( BoltConnector.enabled, TRUE )
                          .withSetting( BoltConnector.listen_address, ":" + port )
                          .withSetting( HttpConnector.enabled, FALSE )
                          .withSetting( GraphDatabaseSettings.auth_enabled, FALSE )
                          .withSetting( GraphDatabaseInternalSettings.databases_root_path,
                                        store.topLevelDirectory().resolve( "data" ).resolve( "databases" ).toString() )
                          .withSetting( GraphDatabaseSettings.default_database, databaseName.name() )
                          .withSetting( GraphDatabaseSettings.transaction_logs_root_path,
                                        store.topLevelDirectory().resolve( "data" ).resolve( "transactions" ).toString() )
                          .writeToFile( neo4jConfigFile );

        Neo4jServerWrapper neo4jServer = new Neo4jServerWrapper( neo4jDir );
        neo4jServer.clearLogs();
        Neo4jServerConnection connection = neo4jServer.start( jvm, neo4jConfigFile, outputRedirect, errorRedirect );
        return new ServerDatabase( neo4jServer, connection.boltUri(), databaseName, connection.pid(), copyLogsToOnClose );
    }

    public static ServerDatabase connectClient( URI boltUri, DatabaseName databaseName, Pid pid )
    {
        return new ServerDatabase( null, boltUri, databaseName, pid, null );
    }

    private final Neo4jServerWrapper neo4jServer;
    private final URI boltUri;
    private final DatabaseName databaseName;
    private final Pid pid;
    private final Driver driver;
    private final Session session;
    private final Path copyLogsToOnClose;
    private final RowsScoreFun rowCountScore;
    private final PlanScoreFun cardinalityScore;

    private ServerDatabase( Neo4jServerWrapper neo4jServer, URI boltUri, DatabaseName databaseName, Pid pid, Path copyLogsToOnClose )
    {
        this.neo4jServer = neo4jServer;
        this.boltUri = Objects.requireNonNull( boltUri );
        this.databaseName = databaseName;
        this.pid = Objects.requireNonNull( pid );
        this.copyLogsToOnClose = copyLogsToOnClose;
        this.rowCountScore = new RowsScoreFun();
        this.cardinalityScore = new PlanScoreFun();
        try
        {
            Config driverConfig = Config.builder()
                                        .withLogging( Logging.none() )
                                        .withoutEncryption()
                                        .build();
            this.driver = GraphDatabase.driver( boltUri,
                                                AuthTokens.none(),
                                                driverConfig );

            this.session = driver.session( SessionConfig.builder()
                                                        .withDatabase( this.databaseName.name() )
                                                        .build() );
        }
        catch ( Exception e )
        {
            closeDriver();
            throw e;
        }
    }

    public URI boltUri()
    {
        return boltUri;
    }

    @Override
    public Pid pid()
    {
        return pid;
    }

    public Session session()
    {
        return session;
    }

    @Override
    public PlanOperator executeAndGetPlan( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback )
    {
        return executeAndGet( query, parameters, executeInTx, shouldRollback, cardinalityScore );
    }

    @Override
    public int executeAndGetRows( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback )
    {
        return executeAndGet( query, parameters, executeInTx, shouldRollback, rowCountScore );
    }

    private <SCORE> SCORE executeAndGet( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback, ScoreFun<SCORE> scoreFun )
    {
        if ( !executeInTx )
        {
            return scoreFun.compute( session.run( query, parameters ) );
        }
        try ( Transaction tx = session.beginTransaction() )
        {
            SCORE score = scoreFun.compute( tx.run( query, parameters ) );
            if ( shouldRollback )
            {
                tx.rollback();
            }
            else
            {
                tx.commit();
            }
            return score;
        }
    }

    @Override
    public void close() throws TimeoutException
    {
        if ( neo4jServer != null && copyLogsToOnClose != null )
        {
            neo4jServer.copyLogsTo( copyLogsToOnClose );
        }
        closeDriver();
        closerServer();
    }

    private void closeDriver()
    {
        if ( driver != null )
        {
            driver.close();
        }
        if ( session != null )
        {
            session.close();
        }
    }

    private void closerServer() throws TimeoutException
    {
        if ( neo4jServer != null )
        {
            neo4jServer.stop();
        }
    }

    private interface ScoreFun<SCORE>
    {
        SCORE compute( Result result );
    }

    private static class RowsScoreFun implements ServerDatabase.ScoreFun<Integer>
    {
        public Integer compute( Result result )
        {
            int rowCount = 0;
            while ( result.hasNext() )
            {
                Record record = result.next();
                // Use record to avoid JIT dead code elimination
                if ( record != null )
                {
                    rowCount++;
                }
            }
            return rowCount;
        }
    }

    private static class PlanScoreFun implements ServerDatabase.ScoreFun<PlanOperator>
    {
        public PlanOperator compute( Result result )
        {
            return PlannerDescription.toPlanOperator( result.consume().plan() );
        }
    }
}
