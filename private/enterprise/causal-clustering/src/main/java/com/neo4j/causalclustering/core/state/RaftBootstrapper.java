/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.TempBootstrapDir;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.ClusterSystemGraphDbmsModel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

/**
 * Bootstraps a raft group for a core database. A single instance is chosen as the bootstrapper, by the discovery service.
 * <p>
 * To bootstrap, in this context, means to set the initial state of the cluster, e.g.
 * the initial state of the database, the initial members in the raft group, etc.
 * One could view this as the beginning state of the state machine onto which all the
 * commands from the Raft log get applied to.
 * <p>
 * The input to this process comes from various sources, but for example the set of members which
 * should be part of the initial Raft group comes from the discovery service. The initial state
 * of a store can come from a seed store which has been put in the appropriate location by an operator,
 * or an empty store will simply be created with a random store id.
 * <p>
 * Bootstrapping happens exactly once in the life of a database. However, if the operator uses
 * the unbind tool to rid every member of the cluster state, then bootstrapping will happen
 * yet again.
 */
public class RaftBootstrapper
{
    private static final long FIRST_INDEX = 0L;
    private static final long FIRST_TERM = 0L;
    private static final String RAFT_BOOTSTRAP_TAG = "raftBootstrap";

    private final BootstrapContext bootstrapContext;
    private final TemporaryDatabaseFactory tempDatabaseFactory;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final StorageEngineFactory storageEngineFactory;
    private final Config config;
    private final BootstrapSaver bootstrapSaver;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    public RaftBootstrapper( BootstrapContext bootstrapContext, TemporaryDatabaseFactory tempDatabaseFactory,
            PageCache pageCache, FileSystemAbstraction fs, LogProvider logProvider, StorageEngineFactory storageEngineFactory, Config config,
            BootstrapSaver bootstrapSaver, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this.bootstrapContext = bootstrapContext;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.pageCache = pageCache;
        this.fs = fs;
        this.log = logProvider.getLog( getClass() );
        this.storageEngineFactory = storageEngineFactory;
        this.config = config;
        this.bootstrapSaver = bootstrapSaver;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
    }

    public CoreSnapshot bootstrap( Set<RaftMemberId> raftMembers )
    {
        return bootstrap( raftMembers, null );
    }

    public CoreSnapshot bootstrap( Set<RaftMemberId> raftMembers, StoreId storeId )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( RAFT_BOOTSTRAP_TAG ) )
        {
            log.info( "Bootstrapping " + bootstrapContext.databaseId() + " for members " + raftMembers );
            if ( isStorePresent() )
            {
                ensureRecoveredOrThrow( bootstrapContext, config, memoryTracker );

                if ( bootstrapContext.databaseId().isSystemDatabase() )
                {
                    bootstrapExistingSystemDatabase();
                }
            }
            else
            {
                createStore( storeId, cursorTracer, bootstrapContext.databaseId().isSystemDatabase() );
            }
            appendNullTransactionLogEntryToSetRaftIndexToMinusOne( bootstrapContext, cursorTracer );
            CoreSnapshot snapshot = buildCoreSnapshot( raftMembers );
            log.info( "Bootstrapping of " + bootstrapContext.databaseId() + " completed " + snapshot );
            return snapshot;
        }
        catch ( Exception e )
        {
            throw new BootstrapException( bootstrapContext.databaseId(), e );
        }
    }

    public void saveStore() throws IOException
    {
        DatabaseLayout databaseLayout = bootstrapContext.databaseLayout();
        bootstrapSaver.save( databaseLayout );
    }

    /**
     * Copies store files and transaction logs of the system database seed, typically into
     *   $NEO4J_HOME/data/databases/system/temp-bootstrap/system
     *
     * upon which a temporary DBMS will be started, so that the seed database
     * can be modified using {@link ClusterSystemGraphDbmsModel#clearClusterProperties(GraphDatabaseService)}.
     *
     * After the database has been updated, the store ID is changed on this instance
     * because the seed is now different from that of other instances. Other instances
     * should delete their system databases and store copy this one as part of their
     * binding process.
     *
     * The database is then copied back into the regular place, typically
     *   $NEO4J_HOME/data/databases/system
     *
     * before the startup process continues.
     */
    private void bootstrapExistingSystemDatabase() throws IOException
    {
        try ( var bootstrapRootDir = TempBootstrapDir.cleanBeforeAndAfter( fs, bootstrapContext.databaseLayout() ) )
        {
            Path tempDefaultDatabaseDir = bootstrapRootDir.get().resolve( SYSTEM_DATABASE_NAME );

            fs.copyRecursively( bootstrapContext.databaseLayout().databaseDirectory(), tempDefaultDatabaseDir );
            fs.copyRecursively( bootstrapContext.databaseLayout().getTransactionLogsDirectory(), tempDefaultDatabaseDir );

            DatabaseLayout tempDatabaseLayout = initializeStoreUsingTempDatabase( bootstrapRootDir.get(), true );

            bootstrapContext.replaceWith( tempDatabaseLayout.databaseDirectory().toFile() );
        }
    }

    private boolean isStorePresent()
    {
        return storageEngineFactory.storageExists( fs, bootstrapContext.databaseLayout(), pageCache );
    }

    private void createStore( StoreId storeId, PageCursorTracer cursorTracer, boolean isSystemDatabase ) throws IOException
    {
        try ( var bootstrapRootDir = TempBootstrapDir.cleanBeforeAndAfter( fs, bootstrapContext.databaseLayout() ) )
        {
            log.info( "Initializing the store for " + bootstrapContext.databaseId() + " using a temporary database in " + bootstrapRootDir );
            DatabaseLayout bootstrapDatabaseLayout = initializeStoreUsingTempDatabase( bootstrapRootDir.get(), isSystemDatabase );
            if ( storeId != null )
            {
                log.info( "Changing store ID of bootstrapped database to " + storeId );
                MetaDataStore.setStoreId( pageCache, bootstrapDatabaseLayout.metadataStore(), storeId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP,
                        cursorTracer );
            }
            log.info( "Moving created store files from " + bootstrapDatabaseLayout + " to " + bootstrapContext.databaseLayout() );
            bootstrapContext.replaceWith( bootstrapDatabaseLayout.databaseDirectory().toFile() );

            // delete transaction logs so they will be recreated with the new store id, they should be empty so it's fine
            bootstrapContext.removeTransactionLogs();
        }
    }

    private DatabaseLayout initializeStoreUsingTempDatabase( Path bootstrapRootDir, boolean isSystem )
    {
        DatabaseLayout databaseLayout;
        try ( TemporaryDatabase tempDatabase = tempDatabaseFactory.startTemporaryDatabase( bootstrapRootDir, config, isSystem ) )
        {
            if ( isSystem )
            {
                ClusterSystemGraphDbmsModel.clearClusterProperties( tempDatabase.graphDatabaseService() );
            }
            databaseLayout = tempDatabase.databaseDirectory();
        }
        return databaseLayout;
    }

    private void ensureRecoveredOrThrow( BootstrapContext bootstrapContext, Config config, MemoryTracker memoryTracker ) throws Exception
    {
        if ( Recovery.isRecoveryRequired( fs, bootstrapContext.databaseLayout(), config, memoryTracker ) )
        {
            String message = "Cannot bootstrap " + bootstrapContext.databaseId() + ". " +
                             "Recovery is required. " +
                             "Please ensure that the store being seeded comes from a cleanly shutdown instance of Neo4j or a Neo4j backup";
            log.error( message );
            throw new IllegalStateException( message );
        }
    }

    private CoreSnapshot buildCoreSnapshot( Set<RaftMemberId> raftMembers )
    {
        var raftCoreState = new RaftCoreState( new MembershipEntry( FIRST_INDEX, raftMembers ) );
        var sessionTrackerState = new GlobalSessionTrackerState();

        var coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, raftCoreState );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTrackerState );
        coreSnapshot.add( CoreStateFiles.LEASE, ReplicatedLeaseState.INITIAL_LEASE_STATE );
        return coreSnapshot;
    }

    /**
     * For the purpose of idempotent application from Raft log to the transaction log, every entry in the transaction log
     * carries in its header the corresponding Raft log index. At bootstrap time an empty transaction log entry denoting
     * the beginning of time (Raft log index -1) is created. This is used during recovery by the Raft machinery to pick up
     * where it left off. It is also highly useful for debugging.
     */
    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne( BootstrapContext bootstrapContext,
            PageCursorTracer cursorTracer ) throws IOException
    {
        DatabaseLayout layout = bootstrapContext.databaseLayout();
        try ( DatabasePageCache databasePageCache = new DatabasePageCache( pageCache, EmptyVersionContextSupplier.EMPTY,
                bootstrapContext.databaseId().name() ) )
        {
            StoreId storeId = storageEngineFactory.storeId( layout, pageCache, cursorTracer );
            TransactionIdStore readOnlyTransactionIdStore = storageEngineFactory.readOnlyTransactionIdStore( fs, layout, databasePageCache,
                    cursorTracer );
            LogFiles logFiles = LogFilesBuilder
                    .activeFilesBuilder( layout, fs, databasePageCache )
                    .withConfig( config )
                    .withStoreId( storeId )
                    .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                    .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                    .build();

            long dummyTransactionId;
            LogPosition currentPosition;
            try ( Lifespan ignored = new Lifespan( logFiles ) )
            {
                LogFile logFile = logFiles.getLogFile();
                TransactionLogWriter writer = logFile.getTransactionLogWriter();

                long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
                PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
                byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
                tx.setHeader( txHeaderBytes, -1, lastCommittedTransactionId, -1, -1, AUTH_DISABLED );

                dummyTransactionId = lastCommittedTransactionId + 1;
                currentPosition = writer.getCurrentPosition();
                writer.append( tx, dummyTransactionId, BASE_TX_CHECKSUM );
                logFile.flush();
            }

            try ( MetadataProvider metadataProvider = storageEngineFactory.transactionMetaDataStore( fs, layout, config, databasePageCache,
                    NULL ) )
            {
                metadataProvider.setLastCommittedAndClosedTransactionId( dummyTransactionId, 0, currentTimeMillis(),
                        currentPosition.getByteOffset(), currentPosition.getLogVersion(), cursorTracer );
            }
        }
    }
}
