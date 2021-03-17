/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.AgentState.State;
import com.neo4j.bench.agent.client.AgentDeployer;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.infra.Compressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.UUID;

public class StopDatabaseCommand extends AgentCommand<String>
{
    private static final Logger LOG = LoggerFactory.getLogger( StopDatabaseCommand.class );

    public StopDatabaseCommand( AgentLifecycle agentLifecycle )
    {
        super( agentLifecycle );
    }

    @Override
    State requiredState()
    {
        return State.STARTED;
    }

    @Override
    State resultState()
    {
        return State.INITIALIZED;
    }

    @Override
    String perform()
    {
        Iterable<AutoCloseable> closeables = agentLifecycle.closeables();
        for ( Iterator<AutoCloseable> i = closeables.iterator(); i.hasNext(); )
        {
            try
            {
                i.next().close();
            }
            catch ( Exception e )
            {
                LOG.warn( "Problem shutting down database server", e );
            }
            i.remove();
        }
        LOG.info( "Database stopped" );

        Path resultsDir = agentLifecycle.results();
        copyAgentLogs( AgentDeployer.AGENT_ERR, resultsDir );
        copyAgentLogs( AgentDeployer.AGENT_OUT, resultsDir );
        Path resultsArchive = agentLifecycle.workspace().resolve( "static" ).resolve( UUID.randomUUID().toString() + ".tar.gz" );
        Compressor.compress( resultsArchive, resultsDir );
        BenchmarkUtil.deleteDir( resultsDir );
        return resultsArchive.getFileName().toString();
    }

    private void copyAgentLogs( String logName, Path resultsDir )
    {
        try
        {
            Path logFile = Paths.get( logName );
            if ( logFile.toFile().exists() )
            {
                Files.copy( logFile, resultsDir.resolve( logFile.getFileName() ) );
            }
        }
        catch ( IOException ioe )
        {
            LOG.warn( "Unable to copy agent logs", ioe );
        }
    }
}
