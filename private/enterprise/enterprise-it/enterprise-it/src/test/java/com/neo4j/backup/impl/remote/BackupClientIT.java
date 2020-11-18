/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.remote;

import com.neo4j.backup.impl.BackupOutputMonitor;
import com.neo4j.backup.impl.BackupsLifecycle;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.recordstorage.ReadOnlyTransactionIdStore;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@PageCacheExtension
@EnterpriseDbmsExtension
class BackupClientIT
{
    @Inject
    TestDirectory testDirectory;
    @Inject
    PageCache pageCache;
    @Inject
    DatabaseManagementService databaseManagementService;
    @Inject
    GraphDatabaseAPI defaultDatabaseAPI;

    private final SocketAddress address = new SocketAddress( "localhost", PortAuthority.allocatePort() );
    private BackupsLifecycle lifecycle;
    private BackupClient backupClient;
    private ThreadPoolJobScheduler scheduler;
    private Server catchupServer;
    private BackupOutputMonitor backupOutputMonitor;

    @BeforeEach
    void setUp()
    {
        lifecycle = BackupsLifecycle.startLifecycle( new RecordStorageEngineFactory(), testDirectory.getFileSystem(), NullLogProvider.nullLogProvider(),
                Clocks.nanoClock(),
                Config.defaults(), PageCacheTracer.NULL );
        scheduler = new ThreadPoolJobScheduler();
        backupClient = new BackupClient( NullLogProvider.nullLogProvider(), lifecycle.getCatchupClientFactory(), testDirectory.getFileSystem(), pageCache,
                                         PageCacheTracer.NULL, new Monitors(), Config.defaults(), new RecordStorageEngineFactory(), Clocks.nanoClock(),
                                         scheduler );
        catchupServer = CatchupServerBuilder.builder()
                .catchupServerHandler( new MultiDatabaseCatchupServerHandler( defaultDatabaseAPI.getDependencyResolver().resolveDependency(
                        DatabaseManager.class ), defaultDatabaseAPI.getDependencyResolver().resolveDependency( DatabaseStateService.class ),
                        testDirectory.getFileSystem(), 32768, NullLogProvider.getInstance(), defaultDatabaseAPI.getDependencyResolver() ) )
                .catchupProtocols( new ApplicationSupportedProtocols( CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .installedProtocolsHandler( null )
                .listenAddress( address ).scheduler( scheduler )
                .config( Config.defaults( CausalClusteringInternalSettings.experimental_catchup_protocol, true ) )
                .bootstrapConfig( serverConfig( Config.defaults() ) )
                .portRegister( new ConnectorPortRegister() )
                .debugLogProvider( NullLogProvider.getInstance() )
                .userLogProvider( NullLogProvider.getInstance() )
                .serverName( "test-catchup-server" )
                .build();

        backupOutputMonitor = new BackupOutputMonitor( NullLogProvider.nullLogProvider(), Clocks.nanoClock() );

        catchupServer.start();
    }

    @AfterEach
    void tearDown() throws Exception
    {
        catchupServer.stop();
        IOUtils.closeAll( scheduler, lifecycle );
    }

    @Test
    void shouldPrepare() throws Exception
    {
        var remoteInfo = backupClient.prepareToBackup( address, defaultDatabaseAPI.databaseName() );
        assertThat( remoteInfo )
                .withFailMessage( "Expected " + remoteInfo + " to have " + address + " and " + defaultDatabaseAPI.databaseId() )
                .matches( ri -> ri.address().equals( address ) )
                .matches( ri -> ri.namedDatabaseId().equals( defaultDatabaseAPI.databaseId() ) );
    }

    @Test
    void shouldFailToPrepare()
    {
        var exception = assertThrows( DatabaseIdDownloadFailedException.class, () -> backupClient.prepareToBackup( address, "foo" ) );
        assertThat( exception.getCause() ).hasMessageContaining( "Database 'foo' does not exist" );
    }

    @Test
    void shouldDownloadFullStoreThenDoIncrementalBackup() throws Exception
    {
        createNode( defaultDatabaseAPI );
        var remoteInfo = backupClient.prepareToBackup( address, defaultDatabaseAPI.databaseName() );
        var backup = DatabaseLayout.ofFlat( testDirectory.directory( "backup" ) );
        assertThat( backup.databaseDirectory() ).isEmptyDirectory();
        assertThat( lastCommitTxId( backup ) ).isNotEqualTo( lastCommitTxId( defaultDatabaseAPI.databaseLayout() ) );

        backupClient.fullStoreCopy( remoteInfo, backup, backupOutputMonitor );
        assertThat( lastCommitTxId( backup ) ).isEqualTo( lastCommitTxId( defaultDatabaseAPI.databaseLayout() ) );

        createNode( defaultDatabaseAPI );
        assertThat( lastCommitTxId( backup ) ).isNotEqualTo( lastCommitTxId( defaultDatabaseAPI.databaseLayout() ) );

        backupClient.updateStore( remoteInfo, backup, backupOutputMonitor );
        assertThat( lastCommitTxId( backup ) ).isEqualTo( lastCommitTxId( defaultDatabaseAPI.databaseLayout() ) );
    }

    @Test
    void canGetMetaData()
    {
        var metaData = backupClient.downloadMetaData( address, "neo4j", IncludeMetadata.all );
        assertThat( metaData ).isNotEmpty();
    }

    private long lastCommitTxId( DatabaseLayout backup ) throws IOException
    {
        return new ReadOnlyTransactionIdStore( testDirectory.getFileSystem(), pageCache, backup, PageCursorTracer.NULL ).getLastCommittedTransactionId();
    }

    private static void createNode( GraphDatabaseAPI databaseAPI )
    {
        try ( var tx = databaseAPI.beginTx() )
        {
            tx.execute( "CREATE ()" );
            tx.commit();
        }
    }

    @Test
    void shouldGetAllDatabaseNames() throws Exception
    {
        var allDatabaseNames = backupClient.getAllDatabaseNames( address );

        assertThat( allDatabaseNames ).containsExactlyInAnyOrder( databaseManagementService.listDatabases()
                .stream()
                .map( db -> ((GraphDatabaseAPI) databaseManagementService.database( db )).databaseId() )
                .toArray( NamedDatabaseId[]::new ) );
    }
}
