/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.TransactionBackupServiceProvider;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.apache.commons.codec.Charsets;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static java.util.Collections.emptyList;

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
                .defaultDatabaseName( GraphDatabaseSettings.DEFAULT_DATABASE_NAME )
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
        try ( Reader reader = fsa.openAsReader( file, Charsets.UTF_8 ) )
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

    public static Map<Integer, String> distributeDatabaseNamesToHostNums( int nHosts, Set<String> databaseNames )
    {
        //Max number of hosts per database is (nHosts / nDatabases) or (nHosts / nDatabases) + 1
        int nDatabases = databaseNames.size();
        int maxCapacity = (nHosts % nDatabases == 0) ? (nHosts / nDatabases) : (nHosts / nDatabases) + 1;

        List<String> repeated = databaseNames.stream()
                .flatMap( db -> IntStream.range( 0, maxCapacity ).mapToObj( ignored -> db ) )
                .collect( Collectors.toList() );

        Map<Integer,String> mapping = new HashMap<>( nHosts );

        for ( int hostId = 0; hostId < nHosts; hostId++ )
        {
            mapping.put( hostId, repeated.get( hostId ) );
        }
        return mapping;
    }
}
