/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.backup.impl.OnlineBackupContext;
import com.neo4j.backup.impl.OnlineBackupExecutor;
import com.neo4j.causalclustering.catchup.CatchupServerBuilder;
import com.neo4j.causalclustering.catchup.MultiDatabaseCatchupServerHandler;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Label;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.net.BootstrapConfiguration.serverConfig;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {SuppressOutputExtension.class, LifeExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
@EnterpriseDbmsExtension
class StoreCopyCheckpointMutexIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String PROP_KEY = "prop";

    @Inject
    private TestDirectory directory;
    @Inject
    private LifeSupport life;
    @Inject
    private DatabaseManagementService managementService;

    /**
     * This test springs from a support case where recovery could not finished on backup after file copy was done because
     * of a corrupted native labelscanstore. All stores / indexes based on the {@link GBPTree} must be copied under a
     * {@link StoreCopyCheckPointMutex checkpoint lock} which blocks checkpointing from running during the store copy.
     * The labelscanstore WAS copied under this lock, but then it was accidentally copied again without the lock and
     * the first (good) copy was overwritten by the new (bad) copy on the backup client.
     * <p>
     * One alternative would be to write a stress test that try and make checkpoint run in parallel with the second copy
     * of label scan store. This is not what is happening here. Instead the test simulate the scenario by creating a
     * custom catchup server. This catchup server use a custom file system abstraction that inject data creation and
     * checkpointing into the {@link StoreChannel#read(ByteBuffer)} that is used to read the files we want to copy.
     * In this way we make the test deterministic.
     * <p>
     * Without the bug-fix, this test fails during recovery that happens as part of {@link OnlineBackupExecutor#executeBackup(OnlineBackupContext)},
     * because labelscanstore gbptree can not be traversed correctly. More specifically a {@link TreeInconsistencyException} will be thrown.
     * <p>
     * Note: Native indexes also need to be copied under the mutex, that is why this test also has an index.
     */
    @Test
    void shouldPerformSuccessfulBackupWithCheckpointAttemptsAndNewDataWhileReadingFiles() throws Throwable
    {
        // Given a db with some data safely checkpointed
        var db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        setupSchema( db );
        createSomeData( db );
        var checkPointer = resolve( db, CheckPointer.class );
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Checkpointing of initial data" ) );

        // and a custom catchup server that will simulate heavy workload and continuous checkpointing during store copy.
        var realFS = resolve( db, FileSystemAbstraction.class );
        var injectingFS = new InjectingFileSystemAbstraction( realFS, db, checkPointer );
        var catchupServer = life.add( buildCatchupServer( db, injectingFS ) );

        // When performing a backup, Then store should be consistent
        performBackupWithConsistencyCheck( catchupServer.address() );

        // and we make sure that store copy used code path where we inject data and checkpoint.
        assertTrue( injectingFS.usedInjectingReadPath, "Expected store copy to use code path where we inject data and checkpoint. " +
                                                       "If we fail here, this test need to be rewritten to do injection in the correct place." );
    }

    /**
     * Build custom catchup server using bits and pieces from the real embedded db.
     *
     * @param db The db that we are creating the catchup server against.
     * @param injectingFS The custom fs that will inject data and try checkpoint on every file read.
     * @return Our custom catchup server.
     */
    private static Server buildCatchupServer( GraphDatabaseAPI db, InjectingFileSystemAbstraction injectingFS )
    {
        var databaseManager = resolve( db, DatabaseManager.class );
        var regularCatchupServerHandler = new MultiDatabaseCatchupServerHandler(
                databaseManager,
                injectingFS,
                32768,
                new Log4jLogProvider( System.out ) );

        return CatchupServerBuilder.builder()
                .catchupServerHandler( regularCatchupServerHandler )
                .catchupProtocols( new ApplicationSupportedProtocols( ApplicationProtocolCategory.CATCHUP, emptyList() ) )
                .modifierProtocols( emptyList() )
                .pipelineBuilder( NettyPipelineBuilderFactory.insecure() )
                .installedProtocolsHandler( null )
                .listenAddress( new SocketAddress( "localhost", 0 ) )
                .scheduler( resolve( db, JobScheduler.class ) )
                .config( Config.defaults() )
                .bootstrapConfig( serverConfig( Config.defaults() ) )
                .portRegister( resolve( db, ConnectorPortRegister.class ) )
                .serverName( "fake-catchup-server" )
                .build();
    }

    private void performBackupWithConsistencyCheck( SocketAddress address ) throws Exception
    {
        var backupDir = directory.directoryPath( "backups" );

        var backupContext = OnlineBackupContext.builder()
                .withAddress( address.getHostname(), address.getPort() )
                .withDatabaseName( DEFAULT_DATABASE_NAME )
                .withConsistencyCheck( true )
                .withBackupDirectory( backupDir )
                .withReportsDirectory( backupDir )
                .build();

        var logProvider = (LogProvider) new Log4jLogProvider( System.out );

        var backupExecutor = OnlineBackupExecutor.builder()
                                                 .withUserLogProvider( logProvider )
                                                 .withInternalLogProvider( logProvider )
                                                 .withClock( Clocks.nanoClock() )
                                                 .build();

        backupExecutor.executeBackup( backupContext );
    }

    private static void createSomeData( GraphDatabaseAPI db )
    {
        try ( var tx = db.beginTx() )
        {
            tx.createNode( LABEL ).setProperty( PROP_KEY, 1 );
            tx.commit();
        }
    }

    private static void setupSchema( GraphDatabaseAPI db )
    {
        try ( var tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP_KEY ).create();
            tx.commit();
        }
        try ( var tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static <T> T resolve( GraphDatabaseAPI db, Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }

    private static class InjectingFileSystemAbstraction extends DelegatingFileSystemAbstraction
    {
        final GraphDatabaseAPI db;
        final CheckPointer checkPointer;

        volatile boolean usedInjectingReadPath;

        InjectingFileSystemAbstraction( FileSystemAbstraction delegate, GraphDatabaseAPI db, CheckPointer checkPointer )
        {
            super( delegate );
            this.db = db;
            this.checkPointer = checkPointer;
        }

        @Override
        public StoreChannel read( File fileName ) throws IOException
        {
            return new DelegatingStoreChannel( super.read( fileName ) )
            {
                @Override
                public int read( ByteBuffer dst ) throws IOException
                {
                    usedInjectingReadPath = true;
                    var read = super.read( dst );

                    // This is where we inject data and checkpoint
                    createSomeData( db );
                    checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Checkpoint in the middle of reading file for store copy." ), () -> true );
                    return read;
                }
            };
        }
    }
}
