/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.client.database.Store;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Neo4jConfigBuilder;
import com.neo4j.bench.client.process.Pid;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.macro.execution.database.Neo4jServerWrapper.Neo4jServerConnection;

import java.net.URI;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static java.lang.ProcessBuilder.Redirect;

public class ServerDatabase implements Database
{
    public static ServerDatabase startServer( Jvm jvm,
                                              Path neo4jDir,
                                              Store store,
                                              Path neo4jConfigFile,
                                              Redirect outputRedirect,
                                              Redirect errorRedirect,
                                              Path copyLogsToOnClose )
    {
        Neo4jConfigBuilder.fromFile( neo4jConfigFile )
                   .setBoltUri( generateBoltUriString() )
                   .withSetting( GraphDatabaseSettings.auth_enabled, "false" )
                   .withSetting( GraphDatabaseSettings.database_path, store.graphDbDirectory().toAbsolutePath().toString() )
                   .writeToFile( neo4jConfigFile );

        Neo4jServerWrapper neo4jServer = new Neo4jServerWrapper( neo4jDir );
        neo4jServer.clearLogs();
        Neo4jServerConnection connection = neo4jServer.start( jvm, neo4jConfigFile, outputRedirect, errorRedirect );
        return new ServerDatabase( neo4jServer, connection.boltUri(), connection.pid(), copyLogsToOnClose );
    }

    private static String generateBoltUriString()
    {
        // TODO reviewer: will be changed before merge - need to find smarter solution
        return "127.0.0.1:7687";
    }

    public static ServerDatabase connectClient( URI boltUri, Pid pid )
    {
        return new ServerDatabase( null, boltUri, pid, null );
    }

    private final Neo4jServerWrapper neo4jServer;
    private final URI boltUri;
    private final Pid pid;
    private final Driver driver;
    private final Session session;
    private final Path copyLogsToOnClose;

    private ServerDatabase( Neo4jServerWrapper neo4jServer, URI boltUri, Pid pid, Path copyLogsToOnClose )
    {
        this.neo4jServer = neo4jServer;
        this.boltUri = Objects.requireNonNull( boltUri );
        this.pid = Objects.requireNonNull( pid );
        this.copyLogsToOnClose = copyLogsToOnClose;
        try
        {
            Config driverConfig = Config.builder()
                                        .withLogging( Logging.none() )
                                        .build();
            this.driver = GraphDatabase.driver( boltUri,
                                                AuthTokens.none(),
                                                driverConfig );
            this.session = driver.session();
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

    @Override
    public int execute( String query, Map<String,Object> parameters, boolean inTx, boolean shouldRollback )
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            StatementResult result = tx.run( query, parameters );
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

            if ( shouldRollback )
            {
                tx.failure();
            }
            else
            {
                tx.success();
            }
            return rowCount;
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
}
