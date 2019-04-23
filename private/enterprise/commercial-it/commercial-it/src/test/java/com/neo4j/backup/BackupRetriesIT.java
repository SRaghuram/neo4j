/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.neo4j.backup.impl.BackupSupportingClassesFactory;
import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.catch_up_client_inactivity_timeout;
import static com.neo4j.causalclustering.core.TransactionBackupServiceProvider.BACKUP_SERVER_NAME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.Settings.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@ExtendWith( {SuppressOutputExtension.class, RandomExtension.class, DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class BackupRetriesIT
{
    private static final DatabaseId DB_ID = new DatabaseId( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

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
        logProvider = FormattedLogProvider.toOutputStream( System.out );
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
        OnlineBackupContext context = buildBackupContext();

        executor.executeBackup( context );

        // backup produced a correct store
        assertEquals( DbRepresentation.of( db ), DbRepresentation.of( DatabaseLayout.of( backupsDir.resolve( DB_ID.name() ).toFile() ) ) );

        // all used channels should be closed after backup is done
        assertAll( "All channels should be closed after backup " + channels,
                channels.stream().map( channel -> () -> assertFalse( channel.isActive() ) ) );

        assertThat( "More than one channel should be used due to breaking " + channels,
                channels, hasSize( greaterThan( 1 ) ) );
    }

    private GraphDatabaseAPI startDb()
    {
        File databaseDirectory = testDirectory.storeDir();
        managementService = new TestCommercialDatabaseManagementServiceBuilder( databaseDirectory )
                .setUserLogProvider( logProvider )
                .setConfig( OnlineBackupSettings.online_backup_enabled, TRUE )
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
                    Node currentNode = createNode( db, j );
                    createRelationship( previousNode, currentNode );
                    previousNode = currentNode;
                }
                tx.success();
            }
        }
    }

    private static void createIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( "Person" ) ).on( "id" ).create();
            db.schema().indexFor( label( "Employee" ) ).on( "name" ).create();
            db.schema().indexFor( label( "Employee" ) ).on( "surname" ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, MINUTES );
            tx.success();
        }
    }

    private static Node createNode( GraphDatabaseService db, int idx )
    {
        Node node = db.createNode( label( "Person" ), label( "Employee" ) );
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
                .withSupportingClassesFactory( backupSupportingClassesFactory )
                .build();
    }

    private OnlineBackupContext buildBackupContext()
    {
        Config config = Config.defaults();
        config.augment( catch_up_client_inactivity_timeout, "30s" );

        return OnlineBackupContext.builder()
                .withAddress( backupAddress( db ) )
                .withDatabaseId( DB_ID )
                .withBackupDirectory( backupsDir )
                .withConfig( config )
                .build();
    }

    private static AdvertisedSocketAddress backupAddress( GraphDatabaseAPI db )
    {
        HostnamePort address = db.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class )
                .getLocalAddress( BACKUP_SERVER_NAME );

        return new AdvertisedSocketAddress( address.getHost(), address.getPort() );
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
            super( storageEngineFactory, fileSystemAbstraction, logProvider, monitors );
            this.channels = channels;
        }

        @Override
        protected PipelineWrapper createPipelineWrapper( Config config )
        {
            PipelineWrapper realPipelineWrapper = super.createPipelineWrapper( config );
            return new ChannelTrackingPipelineWrapper( realPipelineWrapper, channels );
        }
    }

    private static class ChannelTrackingPipelineWrapper implements PipelineWrapper
    {
        final PipelineWrapper delegate;
        final Set<Channel> channels;

        ChannelTrackingPipelineWrapper( PipelineWrapper delegate, Set<Channel> channels )
        {
            this.delegate = delegate;
            this.channels = channels;
        }

        @Override
        public List<ChannelHandler> handlersFor( Channel channel ) throws Exception
        {
            channels.add( channel );
            return delegate.handlersFor( channel );
        }

        @Override
        public String name()
        {
            return delegate.name();
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
