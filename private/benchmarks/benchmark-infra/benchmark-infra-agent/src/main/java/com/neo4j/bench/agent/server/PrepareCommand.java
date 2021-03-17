/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.server;

import com.neo4j.bench.agent.AgentState.State;
import com.neo4j.bench.agent.client.PrepareRequest;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.Dataset;
import com.neo4j.bench.infra.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

public class PrepareCommand extends AgentCommand<Boolean>
{
    private static final Logger LOG = LoggerFactory.getLogger( PrepareCommand.class );

    private final PrepareRequest parameters;

    public PrepareCommand( AgentLifecycle agentLifecycle,
                           PrepareRequest parameters )
    {
        super( agentLifecycle );
        this.parameters = parameters;
    }

    @Override
    State requiredState()
    {
        return State.UNINITIALIZED;
    }

    @Override
    State resultState()
    {
        return State.INITIALIZED;
    }

    @Override
    Boolean perform()
    {
        try ( ArtifactStorage artifactStorage = agentLifecycle.createArtifactStorage() )
        {
            Path neo4jTar = artifactStorage.downloadSingleFile( parameters.productArchive(),
                                                                agentLifecycle.workspace(),
                                                                parameters.artifactBaseUri() );
            Extractor.extract( agentLifecycle.workspace(), Files.newInputStream( neo4jTar ) );
            Files.delete( neo4jTar );

            Dataset dataset = artifactStorage.downloadDataset( parameters.datasetBaseUri(),
                                                              parameters.databaseVersion().minorVersion(),
                                                              parameters.datasetName() );
            dataset.extractInto( agentLifecycle.workspace() );
            return Boolean.TRUE;
        }
        catch ( Exception e )
        {
            LOG.error( "Error occurred", e );
            throw new RuntimeException( "Something wrong happened", e );
        }
    }

    @Override
    void after()
    {
        String productName = parameters.productArchive().replaceAll( "\\.(tgz|tar\\.gz)$", "" );
        agentLifecycle.setNames( productName, parameters.datasetName() );
    }
}
