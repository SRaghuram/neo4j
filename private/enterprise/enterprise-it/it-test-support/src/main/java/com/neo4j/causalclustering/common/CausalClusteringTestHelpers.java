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
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.causalclustering.core.RaftServerFactory.RAFT_SERVER_NAME;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
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
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .inactivityTimeout( Duration.of( 10, ChronoUnit.SECONDS ) ).scheduler( scheduler )
                .bootstrapConfig( clientConfig( Config.defaults() ) )
                .debugLogProvider( logProvider )
                .userLogProvider( logProvider )
                .build();
    }

    public static Server getCatchupServer( CatchupServerHandler catchupServerHandler, SocketAddress listenAddress, JobScheduler scheduler )
    {
        return CatchupServerBuilder.builder()
                .catchupServerHandler( catchupServerHandler )
                .catchupProtocols( new ApplicationSupportedProtocols( CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
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
        SocketAddress hostnamePort = graphDatabase
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

    public static void forceReelection( Cluster cluster, String databaseName ) throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();
        CoreClusterMember follower = randomClusterMember( cluster, leader );

        // make the current leader unresponsive
        Server raftServer = raftServer( leader );
        raftServer.stop();

        // trigger an election and await until a new leader is elected
        follower.resolveDependency( databaseName, RaftMachine.class ).triggerElection();
        assertEventually( "Leader re-election did not happen", cluster::awaitLeader, not( equalTo( leader ) ), 2, MINUTES );

        // make the previous leader responsive again
        raftServer.start();
    }

    public static void removeCheckPointFromDefaultDatabaseTxLog( ClusterMember member ) throws IOException
    {
        assertThat( member.isShutdown(), is( true ) );

        var fs = new DefaultFileSystemAbstraction();
        var databaseLayout = member.databaseLayout();
        var txLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        var logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( txLogsDirectory, fs ).build();

        var checkPointsRemoved = 0;
        while ( removeCheckPointFromTxLog( logFiles, fs ) )
        {
            checkPointsRemoved++;
        }

        assertThat( checkPointsRemoved, greaterThan( 0 ) );
    }

    public static Set<String> listDatabases( Cluster cluster ) throws Exception
    {
        var ref = new AtomicReference<Set<String>>();
        cluster.systemTx( ( sys, tx ) ->
        {
            try ( var result = tx.execute( "SHOW DATABASES" ) )
            {
                var databaseNames = result.stream()
                        .map( row -> (String) row.get( "name" ) )
                        .collect( toSet() );
                ref.set( databaseNames );
            }
            tx.commit();
        } );
        return ref.get();
    }

    public static void createDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "CREATE DATABASE " + databaseName );
            tx.commit();
        } );
    }

    public static void startDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "START DATABASE " + databaseName );
            tx.commit();
        } );
    }

    public static void stopDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "STOP DATABASE " + databaseName );
            tx.commit();
        } );
    }

    public static void dropDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( "DROP DATABASE " + databaseName );
            tx.commit();
        } );
    }

    public static void assertDatabaseEventuallyStarted( String databaseName, Cluster cluster ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not started on all members: " + memberDatabaseStates( databaseName, cluster ),
                () -> allMembersHaveDatabaseState( DatabaseAvailability.AVAILABLE, cluster, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyStarted( String databaseName, Set<? extends ClusterMember> members ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not started on all members: " + memberDatabaseStates( databaseName, members ),
                () -> membersHaveDatabaseState( DatabaseAvailability.AVAILABLE, members, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyStopped( String databaseName, Cluster cluster ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not stopped on all members: " + memberDatabaseStates( databaseName, cluster ),
                () -> allMembersHaveDatabaseState( DatabaseAvailability.STOPPED, cluster, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyStopped( String databaseName, Set<ClusterMember> members ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not stopped on all members: " + memberDatabaseStates( databaseName, members ),
                () -> membersHaveDatabaseState( DatabaseAvailability.STOPPED, members, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyDoesNotExist( String databaseName, Cluster cluster ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not absent on all members: " + memberDatabaseStates( databaseName, cluster ),
                () -> allMembersHaveDatabaseState( DatabaseAvailability.ABSENT, cluster, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyDoesNotExist( String databaseName, Set<ClusterMember> members ) throws InterruptedException
    {
        assertEventually( ignore -> "Database is not absent on all members: " + memberDatabaseStates( databaseName, members ),
                () -> membersHaveDatabaseState( DatabaseAvailability.ABSENT, members, databaseName ), is( true ), 1, MINUTES );
    }

    public static void assertUserDoesNotExist( String userName, Cluster cluster ) throws InterruptedException
    {
        assertEventually( ignore -> "User is not absent on all members: " + memberUserStates( cluster ),
                () -> noMembersHaveUser( cluster, userName ), is( true ), 1, MINUTES);
    }

    public static void assertRoleDoesNotExist( String roleName, Cluster cluster ) throws InterruptedException
    {
        assertEventually( ignore -> "Role is not absent on all members: " + memberRoleStates( cluster ),
                () -> noMembersHaveRole( cluster, roleName ), is( true ), 1, MINUTES);
    }

    private static boolean allMembersHaveDatabaseState( DatabaseAvailability expected, Cluster cluster, String databaseName )
    {
        return membersHaveDatabaseState( expected, cluster.allMembers(), databaseName );
    }

    private static boolean membersHaveDatabaseState( DatabaseAvailability expected, Set<? extends ClusterMember> members, String databaseName )
    {
        return members.stream()
                .map( member -> memberDatabaseState( member, databaseName ) )
                .allMatch( availability -> availability == expected );
    }

    private static Map<ClusterMember,DatabaseAvailability> memberDatabaseStates( String databaseName, Cluster cluster )
    {
        return memberDatabaseStates( databaseName, cluster.allMembers() );
    }

    private static Map<ClusterMember,DatabaseAvailability> memberDatabaseStates( String databaseName, Set<? extends ClusterMember> members )
    {
        return members.stream()
                .collect( toMap( identity(), member -> memberDatabaseState( member, databaseName ) ) );
    }

    private static DatabaseAvailability memberDatabaseState( ClusterMember member, String databaseName )
    {
        GraphDatabaseService db;

        var managementService = member.managementService();
        if ( managementService == null )
        {
            return DatabaseAvailability.ABSENT;
        }
        try
        {
            db = managementService.database( databaseName );
        }
        catch ( DatabaseNotFoundException ignored )
        {
            return DatabaseAvailability.ABSENT;
        }

        try ( var tx = db.beginTx() )
        {
            tx.commit();
        }
        catch ( DatabaseShutdownException ignored )
        {
            return DatabaseAvailability.STOPPED;
        }
        catch ( TransactionFailureException ignored )
        {
            // This should be transient!
            return DatabaseAvailability.UNDEFINED;
        }

        return DatabaseAvailability.AVAILABLE;
    }

    private static Map<ClusterMember,Set<String>> memberUserStates( Cluster cluster )
    {
        return cluster.allMembers().stream().collect( toMap( identity(), CausalClusteringTestHelpers::getMemberUsers ) );
    }

    private static boolean noMembersHaveUser( Cluster cluster, String userName )
    {
        Set<String> users = cluster.allMembers().stream().flatMap( m -> getMemberUsers( m ).stream() ).collect( Collectors.toSet() );
        return !users.contains( userName );
    }

    private static Set<String> getMemberUsers( ClusterMember member )
    {
        GraphDatabaseFacade system = member.systemDatabase();
        Set<String> users = new HashSet<>();
        try ( var tx = system.beginTx() )
        {
            Result result = tx.execute( "SHOW USERS" );
            ResourceIterator<Object> user = result.columnAs( "user" );
            while ( user.hasNext() )
            {
                users.add( user.next().toString() );
            }
            tx.commit();
        }
        return users;
    }

    private static Map<ClusterMember,Set<String>> memberRoleStates( Cluster cluster )
    {
        return cluster.allMembers().stream().collect( toMap( identity(), CausalClusteringTestHelpers::getMemberRoles ) );
    }

    private static boolean noMembersHaveRole( Cluster cluster, String roleName )
    {
        Set<String> roles = cluster.allMembers().stream().flatMap( m -> getMemberRoles( m ).stream() ).collect( Collectors.toSet() );
        return !roles.contains( roleName );
    }

    private static Set<String> getMemberRoles( ClusterMember member )
    {
        GraphDatabaseFacade system = member.systemDatabase();
        Set<String> roles = new HashSet<>();
        try ( var tx = system.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            ResourceIterator<Object> role = result.columnAs( "role" );
            while ( role.hasNext() )
            {
                roles.add( role.next().toString() );
            }
            tx.commit();
        }
        return roles;
    }

    public static void stopDiscoveryService( ClusterMember member ) throws Exception
    {
        TopologyService topologyService = member.resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );
        topologyService.stop();
    }

    public static void startDiscoveryService( ClusterMember member ) throws Exception
    {
        TopologyService topologyService = member.resolveDependency( SYSTEM_DATABASE_NAME, TopologyService.class );
        topologyService.start();
    }

    /**
     * Remember to update keep_logical_logs configuration if you want the files to be pruned.
     */
    public static void forceTxLogRotationAndCheckpoint( GraphDatabaseAPI db ) throws IOException
    {
        // a nicety check, which can be removed if tests ever do anything different
        var config = db.getDependencyResolver().resolveDependency( Config.class );
        assertEquals( "false", config.get( keep_logical_logs ) );

        DependencyResolver dependencyResolver = db.getDependencyResolver();
        dependencyResolver.resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
        SimpleTriggerInfo info = new SimpleTriggerInfo( "test" );
        dependencyResolver.resolveDependency( CheckPointer.class ).forceCheckPoint( info );
    }

    private static boolean removeCheckPointFromTxLog( LogFiles logFiles, FileSystemAbstraction fs ) throws IOException
    {
        var logTailScanner = new LogTailScanner( logFiles, new VersionAwareLogEntryReader(), new Monitors() );
        var logTailInformation = logTailScanner.getTailInformation();

        if ( logTailInformation.commitsAfterLastCheckpoint() )
        {
            assertThat( logTailInformation.isRecoveryRequired(), is( true ) );
            return false;
        }
        else
        {
            assertThat( logTailInformation.lastCheckPoint, is( notNullValue() ) );
            var logPosition = logTailInformation.lastCheckPoint.getLogPosition();
            var logFile = logFiles.getLogFileForVersion( logPosition.getLogVersion() );
            var byteOffset = logPosition.getByteOffset();
            fs.truncate( logFile, byteOffset );
            return true;
        }
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
        return member.defaultDatabase().getDependencyResolver().resolveDependency( Server.class, new RaftServerSelectionStrategy() );
    }

    private static class RaftServerSelectionStrategy implements DependencyResolver.SelectionStrategy
    {
        @Override
        public <T> T select( Class<T> type, Iterable<? extends T> candidates ) throws IllegalArgumentException
        {
            assertThat( type, is( Server.class ) );
            return Iterables.stream( candidates )
                    .map( Server.class::cast )
                    .filter( server -> RAFT_SERVER_NAME.equals( server.name() ) )
                    .findFirst()
                    .map( type::cast )
                    .orElseThrow( IllegalStateException::new );
        }
    }

    private enum DatabaseAvailability
    {
        /**
         * The state is undefined (not to be confused with unavailable). Do not assert on this value in tests!
         */
        UNDEFINED,
        /**
         * A database with the specified name does not exist.
         */
        ABSENT,
        /**
         * The database is stopped.
         */
        STOPPED,
        /**
         * The database is available for transactions.
         */
        AVAILABLE,
    }
}
