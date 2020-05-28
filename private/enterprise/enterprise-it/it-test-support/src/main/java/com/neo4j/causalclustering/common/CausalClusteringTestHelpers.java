/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.dbms.EnterpriseOperatorState;
import com.neo4j.dbms.ShowDatabasesHelpers;
import com.neo4j.dbms.ShowDatabasesHelpers.ShowDatabasesResultRow;
import org.assertj.core.api.Condition;
import org.assertj.core.api.HamcrestCondition;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.CharBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
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
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static com.neo4j.causalclustering.core.RaftServerFactory.RAFT_SERVER_NAME;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.clientConfig;
import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.shuffle;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

public final class CausalClusteringTestHelpers
{
    private CausalClusteringTestHelpers()
    {
    }

    public static ServerAddressResolver clusterResolver( Cluster cluster )
    {
        return address ->
        {
            var serverAddresses = cluster
                    .coreMembers()
                    .stream()
                    .map( c -> URI.create( c.routingURI() ) )
                    .map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) )
                    .collect( toList() );
            shuffle( serverAddresses );
            return new LinkedHashSet<>( serverAddresses );
        };
    }

    public static CatchupClientFactory getCatchupClient( LogProvider logProvider, JobScheduler scheduler )
    {
        return CatchupClientBuilder.builder()
                .catchupProtocols( new ApplicationSupportedProtocols( CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .inactivityTimeout( Duration.of( 10, ChronoUnit.SECONDS ) ).scheduler( scheduler )
                .bootstrapConfig( clientConfig( Config.defaults() ) )
                .commandReader( StorageEngineFactory.selectStorageEngine().commandReaderFactory() )
                .debugLogProvider( logProvider )
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
                .config( Config.defaults() )
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
        return format( "%s:%s", hostnamePort.getHostname(), hostnamePort.getPort() );
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
        runWithLeaderDisabled( cluster, databaseName, ( leader, others ) -> null );
    }

    public static <T> T runWithLeaderDisabled( Cluster cluster, DisabledRaftAction<T> disabledMemberAction ) throws Exception
    {
        return runWithLeaderDisabled( cluster, DEFAULT_DATABASE_NAME, disabledMemberAction );
    }

    public static <T> T runWithLeaderDisabled( Cluster cluster, String databaseName, DisabledRaftAction<T> disabledMemberAction ) throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader( databaseName );
        Server raftServer = raftServer( leader );
        raftServer.stop();
        try
        {
            var otherMembers = new ArrayList<>( cluster.coreMembers() );
            otherMembers.remove( leader );
            // trigger an election and await until a new leader is elected
            var follower = randomClusterMember( cluster, leader );
            follower.resolveDependency( databaseName, RaftMachine.class ).triggerElection();
            assertEventually( "Leader re-election did not happen", () -> cluster.awaitLeader( databaseName ),
                    new HamcrestCondition<>( not( equalTo( leader ) ) ), 2, MINUTES );
            return disabledMemberAction.execute( leader, otherMembers );
        }
        finally
        {
            raftServer.start();
        }
    }

    public static void removeCheckPointFromDefaultDatabaseTxLog( ClusterMember member ) throws IOException
    {
        assertThat( member.isShutdown(), is( true ) );

        var fs = new DefaultFileSystemAbstraction();
        var databaseLayout = member.databaseLayout();
        var txLogsDirectory = databaseLayout.getTransactionLogsDirectory();
        var storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        var logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( txLogsDirectory, fs )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .build();

        var checkPointsRemoved = 0;
        while ( removeCheckPointFromTxLog( logFiles, fs, storageEngineFactory.commandReaderFactory() ) )
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
            tx.execute( String.format( "CREATE DATABASE `%s`", databaseName ) );
            tx.commit();
        } );
    }

    public static void startDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( String.format( "START DATABASE `%s`", databaseName ) );
            tx.commit();
        } );
    }

    public static void stopDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            tx.execute( String.format( "STOP DATABASE `%s`", databaseName ) );
            tx.commit();
        } );
    }

    public static void dropDatabaseDumpData( String databaseName, Cluster cluster ) throws Exception
    {
        dropDatabase( databaseName, cluster, true );
    }

    public static void dropDatabase( String databaseName, Cluster cluster ) throws Exception
    {
        dropDatabase( databaseName, cluster, false );
    }

    private static void dropDatabase( String databaseName, Cluster cluster, boolean dumpData ) throws Exception
    {
        cluster.systemTx( ( sys, tx ) ->
        {
            var dataClause = dumpData ? "DUMP DATA" : "DESTROY DATA";
            tx.execute( String.format( "DROP DATABASE `%s` %s", databaseName, dataClause ) );
            tx.commit();
        } );
    }

    public static List<ShowDatabasesResultRow> showDatabases( Cluster cluster ) throws Exception
    {
        var systemLeader = cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        return ShowDatabasesHelpers.showDatabases( systemLeader.managementService() );
    }

    public static void assertDatabaseEventuallyStarted( String databaseName, Cluster cluster )
    {
        assertEventually( () -> "Database is not started on all members: " + memberDatabaseStates( databaseName, cluster ),
                          () -> allMembersHaveDatabaseState( cluster, databaseName ), allStatesMatch( STARTED ), 10, MINUTES );
    }

    public static void assertDatabaseEventuallyStarted( String databaseName, Set<? extends ClusterMember> members )
    {
        assertEventually( () -> "Database is not started on all members: " + memberDatabaseStates( databaseName, members ),
                          () -> membersHaveDatabaseState( members, databaseName ), allStatesMatch( STARTED ), 10, MINUTES );
    }

    public static void assertDatabaseEventuallyStopped( String databaseName, Cluster cluster )
    {
        assertEventually( () -> "Database is not stopped on all members: " + memberDatabaseStates( databaseName, cluster ),
                          () -> allMembersHaveDatabaseState( cluster, databaseName ), allStatesMatch( STOPPED ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyStopped( String databaseName, Set<ClusterMember> members )
    {
        assertEventually( () -> "Database is not stopped on all members: " + memberDatabaseStates( databaseName, members ),
                          () -> membersHaveDatabaseState( members, databaseName ), allStatesMatch( STOPPED ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyDoesNotExist( String databaseName, Cluster cluster )
    {
        assertEventually( () -> "Database is not absent on all members: " + memberDatabaseStates( databaseName, cluster ),
                          () -> allMembersHaveDatabaseState( cluster, databaseName ), allStatesMatch( UNKNOWN ), 1, MINUTES );
    }

    public static void assertDatabaseEventuallyDoesNotExist( String databaseName, Set<ClusterMember> members )
    {
        assertEventually( () -> "Database is not absent on all members: " + memberDatabaseStates( databaseName, members ),
                          () -> membersHaveDatabaseState( members, databaseName ), allStatesMatch( UNKNOWN ), 1, MINUTES );
    }

    private static Condition<List<EnterpriseOperatorState>> allStatesMatch( EnterpriseOperatorState state )
    {
        return new AllStatesMatch( state );
    }

    public static void assertUserDoesNotExist( String userName, Cluster cluster )
    {
        assertEventually( () -> "User is not absent on all members: " + memberUserStates( cluster ),
                          () -> noMembersHaveUserAndNoErrors( cluster, userName ), TRUE, 1, MINUTES );
    }

    public static void assertRoleDoesNotExist( String roleName, Cluster cluster )
    {
        assertEventually( () -> "Role is not absent on all members: " + memberRoleStates( cluster ),
                          () -> noMembersHaveRoleAndNoErrors( cluster, roleName ), TRUE, 1, MINUTES );
    }

    private static List<EnterpriseOperatorState> allMembersHaveDatabaseState( Cluster cluster, String databaseName )
    {
        return membersHaveDatabaseState( cluster.allMembers(), databaseName );
    }

    private static List<EnterpriseOperatorState> membersHaveDatabaseState( Set<? extends ClusterMember> members,
                                                                           String databaseName )
    {
        return members.stream()
                      .map( member -> memberDatabaseState( member, databaseName ) ).collect( toList() );
    }

    private static Map<ClusterMember,EnterpriseOperatorState> memberDatabaseStates( String databaseName, Cluster cluster )
    {
        return memberDatabaseStates( databaseName, cluster.allMembers() );
    }

    private static Map<ClusterMember,EnterpriseOperatorState> memberDatabaseStates( String databaseName, Set<? extends ClusterMember> members )
    {
        return members.stream()
                      .collect( toMap( identity(), member -> memberDatabaseState( member, databaseName ) ) );
    }

    private static EnterpriseOperatorState memberDatabaseState( ClusterMember member, String databaseName )
    {
        GraphDatabaseFacade database;
        try
        {
            database = member.database( databaseName );
        }
        catch ( DatabaseNotFoundException e )
        {
            return EnterpriseOperatorState.UNKNOWN;
        }
        var databaseStateService = member.resolveDependency( SYSTEM_DATABASE_NAME, DatabaseStateService.class );
        return (EnterpriseOperatorState) databaseStateService.stateOfDatabase( database.databaseId() );
    }

    private static Map<ClusterMember,Set<String>> memberUserStates( Cluster cluster )
    {
        return cluster.allMembers().stream().collect( toMap( identity(), CausalClusteringTestHelpers::getMemberUsers ) );
    }

    private static boolean noMembersHaveUserAndNoErrors( Cluster cluster, String userName )
    {
        try
        {
            Set<String> users = cluster.allMembers().stream().flatMap( m -> getMemberUsers( m ).stream() ).collect( Collectors.toSet() );
            return !users.contains( userName );
        }
        catch ( Exception ignore )
        {
            return false;
        }
    }

    private static Set<String> getMemberUsers( ClusterMember member )
    {
        return getNodeNames(member, "User");
    }

    private static Map<ClusterMember,Set<String>> memberRoleStates( Cluster cluster )
    {
        return cluster.allMembers().stream().collect( toMap( identity(), CausalClusteringTestHelpers::getMemberRoles ) );
    }

    private static boolean noMembersHaveRoleAndNoErrors( Cluster cluster, String roleName )
    {
        try
        {
            Set<String> roles = cluster.allMembers().stream().flatMap( m -> getMemberRoles( m ).stream() ).collect( Collectors.toSet() );
            return !roles.contains( roleName );
        }
        catch ( Exception ignore )
        {
            return false;
        }
    }

    private static Set<String> getMemberRoles( ClusterMember member )
    {
        return getNodeNames(member, "Role");
    }

    private static Set<String> getNodeNames( ClusterMember member, String label )
    {
        GraphDatabaseFacade system = member.systemDatabase();
        Set<String> nodeNames = new HashSet<>();
        try ( Transaction tx = system.beginTx() )
        {
            ResourceIterator<Node> nodes = tx.findNodes( Label.label( label ) );
            while ( nodes.hasNext() )
            {
                var node = nodes.next();
                var name = node.getProperty( "name" );
                nodeNames.add( name.toString() );
            }
            tx.commit();
        }
        return nodeNames;
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

    private static boolean removeCheckPointFromTxLog( LogFiles logFiles, FileSystemAbstraction fs,
            CommandReaderFactory commandReaderFactory ) throws IOException
    {
        var logTailScanner = new LogTailScanner( logFiles, new VersionAwareLogEntryReader( commandReaderFactory ), new Monitors(), INSTANCE );
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

    private static Server raftServer( ClusterMember member )
    {
        return member.defaultDatabase().getDependencyResolver().resolveDependency( Server.class, new RaftServerSelectionStrategy() );
    }

    public interface DisabledRaftAction<T>
    {
        T execute( CoreClusterMember oldLeader, List<CoreClusterMember> otherMembers ) throws Exception;
    }

    private static class RaftServerSelectionStrategy implements DependencyResolver.SelectionStrategy
    {
        @Override
        public <T> T select( Class<T> type, Iterable<? extends T> candidates )
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

    private static class AllStatesMatch extends Condition<List<EnterpriseOperatorState>>
    {
        private AllStatesMatch( EnterpriseOperatorState state )
        {
            super( enterpriseOperatorStates -> enterpriseOperatorStates.stream().allMatch( operatorState -> operatorState == state ),
                   "Expected all databases to have operator state " + state );
        }
    }
}
