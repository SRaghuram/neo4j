/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent.client;

import com.neo4j.bench.common.util.Retrier;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.future.AuthFuture;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.scp.client.DefaultScpClient;
import org.apache.sshd.server.forward.AcceptAllForwardingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import static com.neo4j.bench.agent.Agent.DEFAULT_WORK_DIRECTORY;
import static com.neo4j.bench.agent.server.AgentInstance.STARTED_MESSAGE;
import static java.lang.String.format;

public class AgentDeployer
{
    /**
     * This should equal to the 'project.artifactId' in pom.xml
     */
    public static final String AGENT_JAR_NAME = "benchmark-infra-agent";
    public static final String AGENT_OUT = "/tmp/agent-out.log";
    public static final String AGENT_ERR = "/tmp/agent-error.log";
    public static final int DEFAULT_SSH_PORT = 22;

    private static final Logger LOG = LoggerFactory.getLogger( AgentDeployer.class );
    private static final Duration STARTUP_DURATION = Duration.ofSeconds( 15 );

    private final InetAddress inetAddress;
    private final Path pathToAgent;
    private final int advertisedSshPort;
    private final int advertisedWebPort;

    public AgentDeployer( InetAddress inetAddress, Path pathToAgent, int advertisedSshPort, int advertisedWebPort )
    {
        this.inetAddress = inetAddress;
        this.pathToAgent = pathToAgent.resolve( AGENT_JAR_NAME + ".jar" );
        this.advertisedSshPort = advertisedSshPort;
        this.advertisedWebPort = advertisedWebPort;
    }

    public URI deploy( List<String> additionalParameters ) throws IOException
    {
        if ( !Files.isRegularFile( pathToAgent ) )
        {
            throw new RuntimeException( "Cannot find file: " + pathToAgent.toAbsolutePath() );
        }
        try ( SshClient sshClient = SshClient.setUpDefaultClient() )
        {
            sshClient.setForwardingFilter( AcceptAllForwardingFilter.INSTANCE );
            sshClient.start();
            ConnectFuture connectFuture = sshClient.connect( "root", inetAddress.getHostAddress(), advertisedSshPort );
            connectFuture.await();
            try ( ClientSession session = connectFuture.getSession() )
            {
                // TODO make key based authentication
                session.addPasswordIdentity( "root" );
                AuthFuture authFuture = session.auth();
                if ( !authFuture.verify().await() )
                {
                    throw new RuntimeException( "cannot auth to host", authFuture.getException() );
                }
                LOG.info( "Connected to remote machine {}:{} via SSH", inetAddress, advertisedSshPort );

                String destinationPath = "/tmp/" + AGENT_JAR_NAME + ".jar";

                execute( session, createCleanupCommand( destinationPath ) );
                LOG.debug( "Cleanup done" );
                upload( session, destinationPath );
                LOG.debug( "Upload done" );
                execute( session, createStartCommand( destinationPath, additionalParameters ) );
                LOG.debug( "Agent starting" );
                checkStarted( session );
                LOG.debug( "Agent started" );
            }
        }
        URI agentUri = URI.create( format( "http://%s:%d/", inetAddress.getHostName(), advertisedWebPort ) );
        LOG.info( "Agent deployed and started on remote machine {}:{} via SSH, agent is available at {}", inetAddress, advertisedSshPort, agentUri );
        return agentUri;
    }

    private void upload( ClientSession session, String destinationPath ) throws IOException
    {
        DefaultScpClient client = new DefaultScpClient( session );
        client.upload( pathToAgent, destinationPath );
    }

    private String execute( ClientSession session, String command ) throws IOException
    {
        try ( ClientChannel channel = session.createExecChannel( command ) )
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            channel.setOut( out );
            channel.open();
            channel.waitFor( EnumSet.of( ClientChannelEvent.CLOSED), Duration.ofSeconds( 5 ) );
            return new String( out.toByteArray() );
        }
    }

    private void checkStarted( ClientSession session )
    {
        new Retrier( STARTUP_DURATION ).retryUntil( () -> countStartupMessages( session ), count -> count > 0, 100 );
    }

    private long countStartupMessages( ClientSession session )
    {
        try
        {
            String log = execute( session, "cat " + AGENT_OUT );
            return Stream.of( log.split( "\n" ) ).filter( line -> line.endsWith( STARTED_MESSAGE ) ).count();
        }
        catch ( IOException e )
        {
            LOG.warn( "Unable to fetch agent's log", e );
            return -1;
        }
    }

    private String createCleanupCommand( String destinationPath )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "rm -rf " );
        sb.append( destinationPath ).append( ' ' );
        sb.append( AGENT_ERR ).append( ' ' );
        sb.append( AGENT_OUT ).append( ' ' );
        sb.append( DEFAULT_WORK_DIRECTORY ).append( ' ' );
        sb.append( "; pkill -9 java" );
        return sb.toString();
    }

    private String createStartCommand( String destinationPath, List<String> additionalParameters )
    {
        StringBuilder sb = new StringBuilder( "/bin/sh -c '" );
        sb.append( "java -jar " );
        sb.append( destinationPath );
        sb.append( " run-server-agent " );
        additionalParameters.forEach( parameter -> sb.append( parameter ).append( ' ' ) );
        sb.append( " >" ).append( AGENT_OUT );
        sb.append( " 2>" ).append( AGENT_ERR );
        return sb.append( '\'' ).toString();
    }
}
