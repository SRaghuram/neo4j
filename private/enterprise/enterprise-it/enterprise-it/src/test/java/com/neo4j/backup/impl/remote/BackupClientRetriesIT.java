/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.remote;

import com.neo4j.backup.impl.BackupOutputMonitor;
import com.neo4j.causalclustering.catchup.CatchupClientBuilder;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.protocol.ClientNettyPipelineBuilder;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
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
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.common.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@PageCacheExtension
@EnterpriseDbmsExtension( configurationCallback = "enableBackup" )
@ExtendWith( {SuppressOutputExtension.class, RandomExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
class BackupClientRetriesIT
{
    private static final String DB_NAME = DEFAULT_DATABASE_NAME;

    @Inject
    private static RandomRule random;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;
    @Inject
    private GraphDatabaseAPI db;

    private final Set<Channel> channels = ConcurrentHashMap.newKeySet();
    private FileSystemAbstraction fs;
    private LogProvider logProvider;
    private DatabaseLayout backupLocation;
    private CatchupClientFactory channelBreakingCatchupFactory;
    private Monitors monitors;
    private ThreadPoolJobScheduler scheduler;
    private BackupClient backupClient;

    @ExtensionCallback
    void enableBackup( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( OnlineBackupSettings.online_backup_enabled, true );
    }

    @BeforeEach
    void setUp()
    {
        fs = testDirectory.getFileSystem();
        logProvider = new Log4jLogProvider( System.out );
        backupLocation = DatabaseLayout.ofFlat( testDirectory.directory( "backups" ).resolve( DB_NAME ) );

        scheduler = new ThreadPoolJobScheduler();
        var config = Config.defaults();
        var supportedProtocolCreator = new SupportedProtocolCreator( config, logProvider );
        StorageEngineFactory storageEngineFactory = new RecordStorageEngineFactory();
        var inactivityTimeout = Duration.ofSeconds( 30 );
        channelBreakingCatchupFactory = CatchupClientBuilder
                .builder()
                .catchupProtocols( supportedProtocolCreator.getSupportedCatchupProtocolsFromConfiguration() )
                .modifierProtocols( supportedProtocolCreator.createSupportedModifierProtocols() )
                .pipelineBuilder( new ChannelTrackingPipelineBuilderFactory( null, channels ) )
                .inactivityTimeout( inactivityTimeout )
                .scheduler( scheduler )
                .config( config )
                .bootstrapConfig( BootstrapConfiguration.clientConfig( config ) )
                .commandReader( storageEngineFactory.commandReaderFactory() )
                .handShakeTimeout( config.get( CausalClusteringSettings.handshake_timeout ) )
                .clock( Clock.systemUTC() )
                .debugLogProvider( logProvider ).build();
        channelBreakingCatchupFactory.init();
        channelBreakingCatchupFactory.start();
        monitors = new Monitors();
        backupClient =
                new BackupClient( logProvider, channelBreakingCatchupFactory, fs, pageCache, PageCacheTracer.NULL, monitors, config,
                        storageEngineFactory,
                        Clocks.nanoClock() );
    }

    @AfterEach
    void tearDown()
    {
        channelBreakingCatchupFactory.stop();
        channelBreakingCatchupFactory.shutdown();
        scheduler.shutdown();
    }

    @Test
    void shouldRetryBackupOfStandaloneDatabaseWhenItFails() throws Exception
    {
        populate( db );

        ChannelBreakingStoreCopyClientMonitor channelBreakingMonitor = buildChannelBreakingStoreCopyMonitor( channels );
        monitors.addMonitorListener( channelBreakingMonitor );

        var storeId = new StoreFiles( fs, pageCache ).readStoreId( db.databaseLayout(), PageCursorTracer.NULL );
        backupClient.fullStoreCopy( new RemoteInfo( backupAddress( db ), storeId, db.databaseId() ), backupLocation,
                new BackupOutputMonitor( logProvider, Clocks.nanoClock() ) );
        // backup produced a correct store
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( backupLocation ) );

        assertThat( channels ).withFailMessage( "More than one channel should be used due to breaking " + channels ).hasSizeGreaterThan( 1 );
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
            tx.schema().awaitIndexesOnline( 2, MINUTES );
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
            String storeFileName = Path.of( file ).getFileName().toString();
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
