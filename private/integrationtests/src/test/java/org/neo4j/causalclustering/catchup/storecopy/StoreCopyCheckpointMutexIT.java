/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.backup.impl.OnlineBackupCommandBuilder;
import org.neo4j.backup.impl.SelectedBackupProtocol;
import org.neo4j.causalclustering.catchup.CatchupServerBuilder;
import org.neo4j.causalclustering.catchup.CheckPointerService;
import org.neo4j.causalclustering.catchup.RegularCatchupServerHandler;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( {TestDirectoryExtension.class, SuppressOutputExtension.class} )
class StoreCopyCheckpointMutexIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String PROP_KEY = "prop";

    @Inject
    private TestDirectory directory;

    /**
     * This test springs from a support case where recovery could not finished on backup after file copy was done because
     * of a corrupted native labelscanstore. All stores / indexes based on the {@link GBPTree} must be copied under a
     * {@link StoreCopyCheckPointMutex checkpoint lock} which blocks checkpointing from running during the store copy.
     * The labelscanstore WAS copied under this lock, but then it was accidentally copied again without the lock and
     * the first (good) copy was overwritten by the new (bad) copy on the backup client.
     *
     * One alternative would be to write a stress test that try and make checkpoint run in parallel with the second copy
     * of label scan store. This is not what is happening here. Instead the test simulate the scenario by creating a
     * custom catchup server. This catchup server use a custom file system abstraction that inject data creation and
     * checkpointing into the {@link StoreChannel#read(ByteBuffer)} that is used to read the files we want to copy.
     * In this way we make the test deterministic.
     *
     * Without the bug-fix, this test fails during recovery that happens as part of {@link OnlineBackupCommandBuilder#backup(File, String)},
     * because labelscanstore gbptree can not be traversed correctly. More specifically a {@link TreeInconsistencyException} will be thrown.
     *
     * Note: Native indexes also need to be copied under the mutex, that is why this test also has an index.
     */
    @Test
    void shouldPerformSuccessfulBackupWithCheckpointAttemptsAndNewDataWhileReadingFiles() throws Throwable
    {
        // Given
        final String hostname = "localhost";
        final int port = PortAuthority.allocatePort();
        final File storeDir = directory.storeDir();

        // a db with some data safely checkpointed
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        Lifespan lifespan = new Lifespan();
        try
        {
            setupSchema( db );
            createSomeData( db );
            CheckPointer checkPointer = resolve( db, CheckPointer.class );
            checkPointer.forceCheckPoint( new SimpleTriggerInfo( "Checkpointing of initial data" ) );

            // and a custom catchup server that will simulate heavy workload and continuous checkpointing during store copy.
            FileSystemAbstraction realFS = resolve( db, FileSystemAbstraction.class );
            InjectingFileSystemAbstraction injectingFS = new InjectingFileSystemAbstraction( realFS, db, checkPointer );
            lifespan.add( buildCatchupServer( hostname, port, db, checkPointer, injectingFS ) );

            // When performing a backup, Then store should be consistent
            performBackupWithConsistencyCheck( hostname, port, storeDir );

            // and we make sure that store copy used code path where we inject data and checkpoint.
            assertTrue( injectingFS.usedInjectingReadPath, "Expected store copy to use code path where we inject data and checkpoint. " +
                    "If we fail here, this test need to be rewritten to do injection in the correct place." );
        }
        finally
        {
            db.shutdown();
            lifespan.close();
        }
    }

    /**
     * Build custom catchup server using bits and pieces from the real embedded db.
     * @param hostname Name of host where server is registered.
     * @param port Port where server is listening
     * @param db The db that we are creating the catchup server against.
     * @param checkPointer Check pointer from db.
     * @param injectingFS The custom fs that will inject data and try checkpoint on every file read.
     * @return Our custom catchup server.
     */
    private static Server buildCatchupServer( String hostname, int port, GraphDatabaseAPI db, CheckPointer checkPointer,
            InjectingFileSystemAbstraction injectingFS )
    {
        MetaDataStore metaDataStore = resolve( db, MetaDataStore.class );
        org.neo4j.storageengine.api.StoreId kernelStoreId = metaDataStore.getStoreId();
        StoreId storeId = new StoreId(
                metaDataStore.getCreationTime(),
                kernelStoreId.getRandomId(),
                metaDataStore.getUpgradeTime(),
                kernelStoreId.getUpgradeId() );
        Supplier<StoreId> storeIdSupplier = () -> storeId;
        CheckPointerService checkPointerService = new CheckPointerService( () -> checkPointer, resolve( db, JobScheduler.class ), Group.CHECKPOINT );
        RegularCatchupServerHandler regularCatchupServerHandler = new RegularCatchupServerHandler(
                resolve( db, Monitors.class ),
                FormattedLogProvider.toOutputStream( System.out ),
                storeIdSupplier,
                () -> resolve( db, NeoStoreDataSource.class ),
                () -> true,
                injectingFS,
                null,
                checkPointerService );
        return new CatchupServerBuilder( regularCatchupServerHandler )
                .listenAddress( new ListenSocketAddress( hostname, port ) )
                .build();
    }

    private void performBackupWithConsistencyCheck( String hostname, int port, File storeDir ) throws CommandFailed, IncorrectUsage
    {
        new OnlineBackupCommandBuilder().withSelectedBackupStrategy( SelectedBackupProtocol.CATCHUP )
                .withHost( hostname )
                .withPort( port )
                .withTimeout( TimeUnit.HOURS.toMillis( 1 ) )
                .withConsistencyCheck( true )
                .backup( storeDir, "backup" );
    }

    private static void createSomeData( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( PROP_KEY, 1 );
            tx.success();
        }
    }

    private static void setupSchema( GraphDatabaseAPI db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( PROP_KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private static <T> T resolve( GraphDatabaseAPI db, Class<T> clazz )
    {
        return db.getDependencyResolver().resolveDependency( clazz );
    }

    private class InjectingFileSystemAbstraction extends DelegatingFileSystemAbstraction
    {
        private final GraphDatabaseAPI db;
        private final CheckPointer checkPointer;
        private volatile boolean usedInjectingReadPath;

        InjectingFileSystemAbstraction( FileSystemAbstraction delegate, GraphDatabaseAPI db, CheckPointer checkPointer )
        {
            super( delegate );
            this.db = db;
            this.checkPointer = checkPointer;
        }

        @Override
        public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
        {
            return new DelegatingStoreChannel( super.open( fileName, openMode ) )
            {
                @Override
                public int read( ByteBuffer dst ) throws IOException
                {
                    usedInjectingReadPath = true;
                    int read = super.read( dst );

                    // This is where we inject data and checkpoint
                    createSomeData( db );
                    checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Checkpoint in the middle of reading file for store copy." ), () -> true );
                    return read;
                }
            };
        }
    }
}
