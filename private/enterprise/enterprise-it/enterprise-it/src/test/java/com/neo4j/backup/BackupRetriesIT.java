/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.BackupSupportingClassesFactory;
import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.protocol.ClientNettyPipelineBuilder;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static com.neo4j.configuration.CausalClusteringSettings.catch_up_client_inactivity_timeout;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@TestDirectoryExtension
@ExtendWith( {SuppressOutputExtension.class, RandomExtension.class } )
@ResourceLock( Resources.SYSTEM_OUT )
class BackupRetriesIT
{
    private static final String DB_NAME = DEFAULT_DATABASE_NAME;

    @Inject
    private static RandomRule random;

    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fs;

    private LogProvider logProvider;
    private Path backupsDir;
    private GraphDatabaseAPI db;
    private StorageEngineFactory storageEngineFactory;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        logProvider = new Log4jLogProvider( System.out );
        backupsDir = testDirectory.directory( "backups" ).toPath();
    }

    @AfterEach
    void tearDown()
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldRetryBackupOfStandaloneDatabaseWhenItFails() throws Exception
    {
        db = startDb();
        populate( db );

        Set<Channel> channels = ConcurrentHashMap.newKeySet();
        ChannelBreakingStoreCopyClientMonitor channelBreakingMonitor = buildChannelBreakingStoreCopyMonitor( channels );

        OnlineBackupExecutor executor = buildBackupExecutor( channels, channelBreakingMonitor );
        var contextBuilder = backupContextBuilder();

        executor.executeBackups( contextBuilder );

        // backup produced a correct store
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( DatabaseLayout.ofFlat( backupsDir.resolve( DB_NAME ) ) ) );

        // all used channels should be closed after backup is done
        assertAll( "All channels should be closed after backup " + channels,
                channels.stream().map( channel -> () -> assertFalse( channel.isActive() ) ) );

        assertThat( "More than one channel should be used due to breaking " + channels,
                channels, hasSize( greaterThan( 1 ) ) );
    }

    private GraphDatabaseAPI startDb()
    {
        Path databaseDirectory = testDirectory.homePath();
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( databaseDirectory )
                .setUserLogProvider( logProvider )
                .setConfig( OnlineBackupSettings.online_backup_enabled, true )
                .build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        storageEngineFactory = db.getDependencyResolver().resolveDependency( StorageEngineFactory.class );
        return db;
    }

    private static void populate( GraphDatabaseService db )
    {
        int txCount = random.nextInt( 100, 500 );
        int nodesInTxCount = random.nextInt( 50, 200 );

        createIndexes( db );

        for ( int i = 0; i < txCount; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node previousNode = null;
                for ( int j = 0; j < nodesInTxCount; j++ )
                {
                    Node currentNode = createNode( tx, j );
                    createRelationship( previousNode, currentNode );
                    previousNode = currentNode;
                }
                tx.commit();
            }
        }
    }

    private static void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label( "Person" ) ).on( "id" ).create();
            tx.schema().indexFor( label( "Employee" ) ).on( "name" ).create();
            tx.schema().indexFor( label( "Employee" ) ).on( "surname" ).create();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, MINUTES );
            tx.commit();
        }
    }

    private static Node createNode( Transaction tx, int idx )
    {
        Node node = tx.createNode( label( "Person" ), label( "Employee" ) );
        node.setProperty( "id", idx );
        node.setProperty( "name", "Person-" + idx );
        node.setProperty( "surname", "Employee-" + idx );
        node.setProperty( "salary", idx * 0.42 );
        node.setProperty( "birthday", LocalDateTime.now() );
        return node;
    }

    private static void createRelationship( Node from, Node to )
    {
        if ( from == null || to == null )
        {
            return;
        }

        Relationship relationship = from.createRelationshipTo( to, withName( "KNOWS" ) );
        long id = from.getId() + to.getId();
        relationship.setProperty( "id", id );
        relationship.setProperty( "value", "Value-" + id );
        relationship.setProperty( "startDate", LocalDate.now() );
    }

    private ChannelBreakingStoreCopyClientMonitor buildChannelBreakingStoreCopyMonitor( Set<Channel> channels )
    {
        ChannelBreaker[] channelBreakers = ChannelBreaker.values();

        List<DatabaseFile> storeFiles = Arrays.stream( DatabaseFile.values() )
                .filter( DatabaseFile::hasIdFile )
                .collect( toList() );

        Map<DatabaseFile,ChannelBreaker> storeFileBreakers = new HashMap<>();
        for ( int i = 0; i < random.nextInt( 1, 50 ); i++ )
        {
            storeFileBreakers.put( random.among( storeFiles ), random.among( channelBreakers ) );
        }

        return new ChannelBreakingStoreCopyClientMonitor( channels, storeFileBreakers, logProvider );
    }

    private OnlineBackupExecutor buildBackupExecutor( Set<Channel> channels, StoreCopyClientMonitor channelBreakingMonitor )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( channelBreakingMonitor );

        BackupSupportingClassesFactory backupSupportingClassesFactory = new ChannelTrackingBackupSupportingClassesFactory( logProvider, monitors, fs,
                storageEngineFactory, channels );

        return OnlineBackupExecutor.builder()
                                   .withUserLogProvider( logProvider )
                                   .withInternalLogProvider( logProvider )
                                   .withProgressMonitorFactory( ProgressMonitorFactory.textual( System.out ) )
                                   .withMonitors( monitors )
                                   .withClock( Clocks.nanoClock() )
                                   .withSupportingClassesFactory( backupSupportingClassesFactory )
                                   .build();
    }

    private OnlineBackupContext.Builder backupContextBuilder()
    {
        Config config = Config.defaults();
        config.set( catch_up_client_inactivity_timeout, Duration.ofSeconds( 30 ) );

        return OnlineBackupContext.builder()
                                  .withAddress( backupAddress( db ) )
                                  .withDatabaseNamePattern( DB_NAME )
                                  .withBackupDirectory( backupsDir )
                                  .withReportsDirectory( testDirectory.directory( "reports" ).toPath() )
                                  .withConfig( config );

    }

    private static SocketAddress backupAddress( GraphDatabaseAPI db )
    {
        HostnamePort address = db.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class )
                .getLocalAddress( BACKUP_SERVER_NAME );

        return new SocketAddress( address.getHost(), address.getPort() );
    }

    private static String randomString()
    {
        return random.nextAlphaNumericString( 10, 100 );
    }

    private static ByteBuf randomByteBuf()
    {
        int oneMB = (int) ByteUnit.MebiByte.toBytes( 1 );
        int tenMB = (int) ByteUnit.MebiByte.toBytes( 10 );
        byte[] array = new byte[random.nextInt( oneMB, tenMB )];
        random.nextBytes( array );
        return Unpooled.wrappedBuffer( array );
    }

    private static class ChannelTrackingBackupSupportingClassesFactory extends BackupSupportingClassesFactory
    {
        final Set<Channel> channels;

        ChannelTrackingBackupSupportingClassesFactory( LogProvider logProvider, Monitors monitors, FileSystemAbstraction fileSystemAbstraction,
                StorageEngineFactory storageEngineFactory, Set<Channel> channels )
        {
            super( storageEngineFactory, fileSystemAbstraction, logProvider, monitors, Clocks.nanoClock() );
            this.channels = channels;
        }

        @Override
        protected NettyPipelineBuilderFactory createPipelineBuilderFactory( SslPolicy sslPolicy )
        {
            return new ChannelTrackingPipelineBuilderFactory( sslPolicy, channels );
        }
    }

    private static class ChannelTrackingPipelineBuilderFactory extends NettyPipelineBuilderFactory
    {
        final Set<Channel> channels;

        ChannelTrackingPipelineBuilderFactory( SslPolicy sslPolicy, Set<Channel> channels )
        {
            super( sslPolicy );
            this.channels = channels;
        }

        @Override
        public ClientNettyPipelineBuilder client( Channel channel, Log log )
        {
            channels.add( channel );
            return super.client( channel, log );
        }
    }

    private enum ChannelBreaker
    {
        /**
         * Break the channel by closing it.
         */
        CLOSE_CHANNEL( ChannelPipeline::close ),

        /**
         * Break the channel by faking a {@link RuntimeException}.
         */
        FIRE_RUNTIME_EXCEPTION( pipeline -> pipeline.fireExceptionCaught( new RuntimeException( randomString() ) ) ),

        /**
         * Break the channel by faking a {@link IOException}.
         */
        FIRE_CHECKED_EXCEPTION( pipeline -> pipeline.fireExceptionCaught( new IOException( randomString() ) ) ),

        /**
         * Break the channel by faking a {@link OutOfMemoryError}.
         */
        FIRE_OUT_OF_MEMORY_ERROR( pipeline -> pipeline.fireExceptionCaught( new OutOfMemoryError( randomString() ) ) ),

        /**
         * Break the channel by firing a message of unsupported type {@link Object}.
         */
        FIRE_INBOUND_OBJECT( pipeline -> pipeline.fireChannelRead( new Object() ) ),

        /**
         * Break the channel by firing a message of unsupported type {@link String}.
         */
        FIRE_INBOUND_STRING( pipeline -> pipeline.fireChannelRead( randomString() ) ),

        /**
         * Break the channel by firing an incorrect message containing random bytes.
         */
        FIRE_INBOUND_RANDOM_BYTE_BUF( pipeline -> pipeline.fireChannelRead( randomByteBuf() ) ),

        /**
         * Make the channel stop reading responses to cause timeouts.
         */
        TURN_OFF_AUTO_READ( pipeline -> pipeline.channel().config().setAutoRead( false ) );

        final Consumer<ChannelPipeline> action;

        ChannelBreaker( Consumer<ChannelPipeline> action )
        {
            this.action = action;
        }

        void doBreak( Channel channel )
        {
            action.accept( channel.pipeline() );
        }
    }

    private static class ChannelBreakingStoreCopyClientMonitor extends StoreCopyClientMonitor.Adapter
    {
        final Map<DatabaseFile,ChannelBreaker> storeFileBreakers;
        final Set<Channel> channels;
        final Log log;

        ChannelBreakingStoreCopyClientMonitor( Set<Channel> channels, Map<DatabaseFile,ChannelBreaker> storeFileBreakers, LogProvider logProvider )
        {
            this.storeFileBreakers = new ConcurrentHashMap<>( storeFileBreakers );
            this.channels = channels;
            this.log = logProvider.getLog( getClass() );
        }

        @Override
        public void startReceivingStoreFile( String file )
        {
            String storeFileName = new File( file ).getName();
            DatabaseFile databaseFile = DatabaseFile.fileOf( storeFileName ).orElseThrow( AssertionError::new );
            ChannelBreaker breaker = storeFileBreakers.remove( databaseFile );
            if ( breaker != null )
            {
                log.info( "Breaking receiving of a store file %s using %s", storeFileName, breaker );
                breakChannels( breaker );
            }
        }

        void breakChannels( ChannelBreaker breaker )
        {
            for ( Channel channel : channels )
            {
                if ( channel.isActive() )
                {
                    try
                    {
                        breaker.doBreak( channel );
                    }
                    catch ( Throwable t )
                    {
                        log.warn( "Unable to break channel " + channel + " using " + breaker, t );
                    }
                }
            }
        }
    }
}
