/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.RaftServerFactory.RAFT_SERVER_NAME;
import static com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

public final class CausalClusteringTestHelpers
{
    private CausalClusteringTestHelpers()
    {
    }

    public static CatchupClientFactory getCatchupClient( LogProvider logProvider, JobScheduler scheduler )
    {
        return CatchupClientBuilder.builder()
                .catchupProtocols( new ApplicationSupportedProtocols( CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( new NettyPipelineBuilderFactory( VOID_WRAPPER ) )
                .inactivityTimeout( Duration.of( 10, ChronoUnit.SECONDS ) ).scheduler( scheduler )
                .bootstrapConfig( clientConfig( Config.defaults() ) )
                .debugLogProvider( logProvider )
                .userLogProvider( logProvider )
                .build();
    }

    public static Server getCatchupServer( CatchupServerHandler catchupServerHandler, ListenSocketAddress listenAddress, JobScheduler scheduler )
    {
        return CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .catchupProtocols( new ApplicationSupportedProtocols( CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( new NettyPipelineBuilderFactory( VOID_WRAPPER ) )
                .installedProtocolsHandler( null )
                .listenAddress( listenAddress ).scheduler( scheduler )
                .bootstrapConfig( serverConfig( Config.defaults() ) )
                .portRegister( new ConnectorPortRegister() )
                .debugLogProvider( NullLogProvider.getInstance() )
                .userLogProvider( NullLogProvider.getInstance() )
                .serverName( "test-catchup-server" )
                .build();
    }

    public static String fileContent( File file, FileSystemAbstraction fsa ) throws IOException
    {
        int chunkSize = 128;
        StringBuilder stringBuilder = new StringBuilder();
        try ( Reader reader = fsa.openAsReader( file, UTF_8 ) )
        {
            CharBuffer charBuffer = CharBuffer.wrap( new char[chunkSize] );
            while ( reader.read( charBuffer ) != -1 )
            {
                charBuffer.flip();
                stringBuilder.append( charBuffer );
                charBuffer.clear();
            }
        }
        return stringBuilder.toString();
    }

    public static String transactionAddress( GraphDatabaseAPI graphDatabase )
    {
        AdvertisedSocketAddress hostnamePort = graphDatabase
                .getDependencyResolver()
                .resolveDependency( Config.class )
                .get( CausalClusteringSettings.transaction_advertised_address );
        return String.format( "%s:%s", hostnamePort.getHostname(), hostnamePort.getPort() );
    }

    public static String backupAddress( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class )
                .getLocalAddress( TransactionBackupServiceProvider.BACKUP_SERVER_NAME )
                .toString();
    }

    public static void forceReelection( Cluster cluster ) throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();
        CoreClusterMember follower = randomClusterMember( cluster, leader );

        // make the current leader unresponsive
        Server raftServer = raftServer( leader );
        raftServer.stop();

        // trigger an election and await until a new leader is elected
        follower.raft().triggerElection();
        assertEventually( "Leader re-election did not happen", cluster::awaitLeader, not( equalTo( leader ) ), 2, MINUTES );

        // make the previous leader responsive again
        raftServer.start();
    }

    private static CoreClusterMember randomClusterMember( Cluster cluster, CoreClusterMember except )
    {
        CoreClusterMember[] members = cluster.coreMembers()
                .stream()
                .filter( member -> !member.id().equals( except.id() ) )
                .toArray( CoreClusterMember[]::new );

        return members[ThreadLocalRandom.current().nextInt( members.length )];
    }

    private static Server raftServer( CoreClusterMember member )
    {
        return member.database().getDependencyResolver().resolveDependency( Server.class, new RaftServerSelectionStrategy() );
    }

    private static class RaftServerSelectionStrategy implements DependencyResolver.SelectionStrategy
    {
        @Override
        public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
        {
            assertEquals( Server.class, type );
            return Iterables.stream( candidates )
                    .map( Server.class::cast )
                    .filter( server -> RAFT_SERVER_NAME.equals( server.name() ) )
                    .findFirst()
                    .map( type::cast )
                    .orElseThrow( IllegalStateException::new );
        }
    }
}
