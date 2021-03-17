/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.cli;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.neo4j.bench.agent.Agent;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.agent.server.AgentInstance;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

@Command( name = "run-server-agent", description = "Run agent for remote server" )
public class RunServerAgentCommand implements Runnable
{
    private static final Logger LOG = LoggerFactory.getLogger( RunServerAgentCommand.class );

    private static final String CMD_LISTEN_PORT = "--port";
    private static final String CMD_WORK_DIRECTORY = "--work-dir";

    @Option( type = OptionType.COMMAND,
            name = {CMD_LISTEN_PORT},
            description = "Listen port",
            title = "Listen port" )
    private int port = Agent.DEFAULT_PORT;

    @Option( type = OptionType.COMMAND,
            name = {CMD_WORK_DIRECTORY},
            description = "Path to agent working directory",
            title = "Path to agent working directory" )
    private String workDirectory = Agent.DEFAULT_WORK_DIRECTORY;

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_AWS_REGION,
            title = "AWS Region" )
    private String awsRegion = "eu-north-1";

    @Option( type = OptionType.COMMAND,
            name = InfraParams.CMD_AWS_ENDPOINT_URL,
            title = "AWS endpoint URL, used for testing" )
    private String awsEndpointUrl;

    @Override
    public void run()
    {
        LOG.info( "Agent will be started on {} and using work directory {}", port, workDirectory );
        AgentInstance agentInstance = new AgentInstance( port,
                                                         Paths.get( workDirectory ),
                                                         this::createArtifactStorage,
                                                         DatabaseServerWrapper::neo4j );
        agentInstance.start();
    }

    private ArtifactStorage createArtifactStorage()
    {
        if ( isNotEmpty( awsEndpointUrl ) )
        {
            return AWSS3ArtifactStorage.create( new AwsClientBuilder.EndpointConfiguration( awsEndpointUrl, awsRegion ) );
        }
        else
        {
            return AWSS3ArtifactStorage.create( awsRegion );
        }
    }
}
