/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.AgentState.State;
import com.neo4j.bench.agent.client.StartDatabaseRequest;
import com.neo4j.bench.agent.database.DatabaseServerConnection;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.common.database.AutoDetectStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class StartDatabaseCommand extends AgentCommand<URI>
{
    private static final String AUTH_ENABLED = "dbms.security.auth_enabled";
    private static final String DATABASES_ROOT = "unsupported.dbms.directories.databases.root";
    private static final String TRANSACTIONS_ROOT = "dbms.directories.transaction.logs.root";

    private static final Logger LOG = LoggerFactory.getLogger( StartDatabaseCommand.class );

    private final StartDatabaseRequest parameters;

    public StartDatabaseCommand( AgentLifecycle agentLifecycle,
                                 StartDatabaseRequest parameters )
    {
        super( agentLifecycle );
        this.parameters = parameters;
    }

    @Override
    State requiredState()
    {
        return State.INITIALIZED;
    }

    @Override
    State resultState()
    {
        return State.STARTED;
    }

    @Override
    URI perform()
    {
        Jvm jvm = Jvm.defaultJvmOrFail();
        LOG.debug( "Jvm is going to be: {}", jvm );

        ProcessBuilder.Redirect outputRedirect = ProcessBuilder.Redirect.to( agentLifecycle.results().resolve( "neo4j-out.log" ).toFile() );
        ProcessBuilder.Redirect errorRedirect = ProcessBuilder.Redirect.to( agentLifecycle.results().resolve( "neo4j-error.log" ).toFile() );

        DatabaseServerWrapper databaseServerWrapper = agentLifecycle.getOrCreateDatabaseWrapper();
        Store store = AutoDetectStore.createFrom( agentLifecycle.dataset() );
        if ( parameters.copyStore() )
        {
            store = store.makeTemporaryCopy();
            unbind();
        }
        Neo4jConfig neo4jConfig = parameters.databaseConfig()
                                            .withSetting( AUTH_ENABLED, "false" )
                                            .withSetting( DATABASES_ROOT, store.topLevelDirectory().resolve( "data" ).resolve( "databases" ).toString() )
                                            .withSetting( TRANSACTIONS_ROOT, store.topLevelDirectory().resolve( "data" ).resolve( "transactions" ).toString() );

        Path neo4jConfigFile = agentLifecycle.results().resolve( "neo4j.conf" );
        writeConfig( neo4jConfig, neo4jConfigFile );
        LOG.debug( "Config written" );

        agentLifecycle.addCloseable( agentLifecycle::removeDatabase );
        agentLifecycle.addCloseable( databaseServerWrapper::stop );
        agentLifecycle.addCloseable( () -> databaseServerWrapper.copyLogsTo( agentLifecycle.results() ) );
        agentLifecycle.addCloseable( store );

        DatabaseServerConnection connection = databaseServerWrapper.start( jvm, neo4jConfigFile, outputRedirect, errorRedirect );
        agentLifecycle.setDatabase( parameters.copyStore(), parameters.databaseConfig(), connection );

        LOG.info( "Database started with pid {}, available at {}", connection.pid(), connection.boltUri() );

        return connection.boltUri();
    }

    private void unbind()
    {
        // TODO: unbind for cluster
        LOG.debug( "Unbind complete" );
    }

    public static void writeConfig( Neo4jConfig neo4jConfig, Path neo4jConfigFile )
    {
        List<String> lines = new ArrayList<>();
        neo4jConfig.toMap().forEach( ( key, value ) -> lines.add( key + "=" + value ) );
        neo4jConfig.getJvmArgs().forEach( jvmArg -> lines.add( "dbms.jvm.additional=" + jvmArg ) );
        String contents = String.join( "\n", lines ) + "\n";
        BenchmarkUtil.stringToFile( contents, neo4jConfigFile );
    }
}
