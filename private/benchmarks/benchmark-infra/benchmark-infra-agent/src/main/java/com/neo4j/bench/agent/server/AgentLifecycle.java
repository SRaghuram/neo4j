/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.AgentState;
import com.neo4j.bench.agent.AgentState.State;
import com.neo4j.bench.agent.database.DatabaseServerConnection;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class AgentLifecycle implements AutoCloseable
{
    private static final Logger LOG = LoggerFactory.getLogger( AgentInstance.class );

    private final Path workspace;
    private final Supplier<ArtifactStorage> artifactStorageFactory;
    private final Function<Path,DatabaseServerWrapper> databaseServerWrapperFactory;
    private final List<AutoCloseable> closeables = new LinkedList<>();
    private final Instant agentStartedAt = Clock.systemUTC().instant();

    private State state;

    private String productName;
    private String datasetName;

    private Boolean copyStore;
    private Neo4jConfig databaseConfig;
    private Instant databaseStartedAt;
    private DatabaseServerConnection connection;

    AgentLifecycle( Path workspace,
                    Supplier<ArtifactStorage> artifactStorageFactory,
                    Function<Path,DatabaseServerWrapper> databaseServerWrapperFactory )
    {
        this.workspace = workspace;
        this.artifactStorageFactory = artifactStorageFactory;
        this.databaseServerWrapperFactory = databaseServerWrapperFactory;
        this.state = State.UNINITIALIZED;
        BenchmarkUtil.tryMkDir( workspace );
    }

    void assertState( State expectedState )
    {
        if ( expectedState != state )
        {
            throw new IllegalStateException( "Agent is in state " + state + " for operation state " + expectedState + " is needed" );
        }
    }

    void setState( State resultState )
    {
        this.state = resultState;
    }

    Path workspace()
    {
        return workspace;
    }

    ArtifactStorage createArtifactStorage()
    {
        return artifactStorageFactory.get();
    }

    void setNames( String productName, String datasetName )
    {
        this.productName = productName;
        this.datasetName = datasetName;
    }

    DatabaseServerWrapper getOrCreateDatabaseWrapper()
    {
        return databaseServerWrapperFactory.apply( workspace.resolve( productName ) );
    }

    Path dataset()
    {
        return workspace.resolve( datasetName );
    }

    void addCloseable( AutoCloseable store )
    {
        this.closeables.add( store );
    }

    Path results()
    {
        Path results = workspace.resolve( "results" );
        BenchmarkUtil.tryMkDir( results );
        return results;
    }

    void setDatabase( boolean copyStore, Neo4jConfig databaseConfig, DatabaseServerConnection connection )
    {
        this.copyStore = copyStore;
        this.databaseConfig = databaseConfig;
        this.databaseStartedAt = Clock.systemUTC().instant();
        this.connection = connection;
    }

    void removeDatabase()
    {
        this.copyStore = null;
        this.databaseConfig = null;
        this.databaseStartedAt = null;
        this.connection = null;
    }

    Iterable<AutoCloseable> closeables()
    {
        return closeables;
    }

    public String agentState()
    {
        AgentState agentState = new AgentState( state,
                                                agentStartedAt,
                                                productName,
                                                datasetName,
                                                copyStore,
                                                databaseConfig,
                                                connection == null ? null : connection.boltUri(),
                                                databaseStartedAt );
        return JsonUtil.serializeJson( agentState );
    }

    @Override
    public void close()
    {
        if ( connection != null )
        {
            LOG.warn( "Running process at close {}", connection.pid() );
            kill( connection.pid() );
        }
        removeDatabase();
        productName = null;
        datasetName = null;
        try
        {
            BenchmarkUtil.deleteDir( workspace );
            BenchmarkUtil.tryMkDir( workspace );
        }
        catch ( Exception e )
        {
            LOG.error( "Error during deleting workspace", e );
        }
        state = State.UNINITIALIZED;
    }

    static void kill( Pid pid )
    {
        String[] command = System.getProperty( "os.name" ).toLowerCase().contains( "windows" ) ?
                           new String[]{"taskkill", "/F", "/t", "/FI", "/PID", String.valueOf( pid.get() )} :
                           new String[]{"kill", "-KILL", String.valueOf( pid.get() )};

        try
        {
            new ProcessBuilder( command ).start().waitFor();
        }
        catch ( InterruptedException | IOException e )
        {
            LOG.error( "cannot kill process " + pid, e );
        }
    }
}
