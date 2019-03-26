/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.ClusteredDatabaseContext;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.helper.TemporaryDatabase;
import com.neo4j.causalclustering.helper.TemporaryDatabaseFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.internal.id.IdType.ARRAY_BLOCK;
import static org.neo4j.internal.id.IdType.LABEL_TOKEN;
import static org.neo4j.internal.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.internal.id.IdType.NODE;
import static org.neo4j.internal.id.IdType.NODE_LABELS;
import static org.neo4j.internal.id.IdType.PROPERTY;
import static org.neo4j.internal.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.internal.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.RELATIONSHIP;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.internal.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.internal.id.IdType.SCHEMA;
import static org.neo4j.internal.id.IdType.STRING_BLOCK;

/**
 * Bootstraps the core. A single instance is chosen as the bootstrapper, by the discovery service.
 *
 * To bootstrap, in this context, means to set the initial state of the cluster, e.g.
 * the initial state of the databases it consists of, the initial members in the raft group,
 * etc. One could view this as the beginning state of the state machine onto which all the
 * commands from the Raft log get applied to.
 *
 * The input to this process comes from various sources, but for example the set of members which
 * should be part of the initial Raft group comes from the discovery service. The initial state
 * of a store can come from a seed store which has been put in the appropriate location by an operator,
 * or an empty store will simply be created with a random store id.
 *
 * Bootstrapping happens exactly once in the life of a cluster. However, if the operator uses
 * the unbind tool to rid every member of the cluster state, then bootstrapping will happen
 * yet again.
 *
 * The bootstrapper currently supports the bootstrapping of either one or two databases. The active
 * database (i.e. the regular database accessed by applications) and optionally an additional system
 * database which at the time of writing is utilised as part of a native security solution.
 */
public class CoreBootstrapper
{
    private static final long FIRST_INDEX = 0L;
    private static final long FIRST_TERM = 0L;

    private final ClusteredDatabaseManager<?> clusteredDatabaseManager;
    private final TemporaryDatabaseFactory tempDatabaseFactory;
    private final Function<String,DatabaseInitializer> databaseInitializers;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Log log;
    private final StorageEngineFactory storageEngineFactory;
    private final Config config;

    CoreBootstrapper( ClusteredDatabaseManager<?> clusteredDatabaseManager, TemporaryDatabaseFactory tempDatabaseFactory,
            Function<String,DatabaseInitializer> databaseInitializers, FileSystemAbstraction fs, Config config, LogProvider logProvider, PageCache pageCache,
            StorageEngineFactory storageEngineFactory )
    {
        this.clusteredDatabaseManager = clusteredDatabaseManager;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.databaseInitializers = databaseInitializers;
        this.fs = fs;
        this.pageCache = pageCache;
        this.log = logProvider.getLog( getClass() );
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
    }

    /**
     * Bootstraps the cluster using the supplied set of members.
     *
     * TODO: Completely overhaul. Could maybe take a database context object?
     *
     * @param members the members to bootstrap with (this comes from the discovery service).
     * @return a snapshot which represents the initial state.
     * @throws IOException if an I/O exception occurs.
     */
    public Map<String,CoreSnapshot> bootstrap( Set<MemberId> members ) throws Exception
    {
        prepareForBootstrapping();
        initializeSystemDatabaseIfNeeded();
        appendDummyTransactions();
        return buildCoreSnapshots( members );
    }

    private void prepareForBootstrapping() throws Exception
    {
        for ( ClusteredDatabaseContext db : clusteredDatabaseManager.registeredDatabases().values() )
        {
            DatabaseLayout layout = db.databaseLayout();
            Config config = createTemporaryConfig( db );

            ensureRecoveredOrThrow( layout, config );

            if ( isStorePresent( db ) )
            {
                appendNullTransactionLogEntryToSetRaftIndexToMinusOne( layout, createTemporaryConfig( db ) );
            }
        }
    }

    private void initializeSystemDatabaseIfNeeded()
    {
        boolean systemDbStoreExists = clusteredDatabaseManager.getDatabaseContext( new DatabaseId( SYSTEM_DATABASE_NAME ) )
                .map( this::isStorePresent )
                .orElse( false );

        // find some non-system db and use it to start a temporary database
        // this code and starting a temp db should go away when we have a migration step that introduces a system db
        ClusteredDatabaseContext defaultDb = clusteredDatabaseManager.registeredDatabases()
                .values()
                .stream()
                .filter( db -> !db.databaseName().equals( SYSTEM_DATABASE_NAME ) )
                .findFirst()
                .orElseThrow( IllegalStateException::new );

        File dir = defaultDb.databaseLayout().databaseDirectory();
        Config config = createTemporaryConfig( defaultDb );

        try ( TemporaryDatabase temporaryDatabase = tempDatabaseFactory.startTemporaryDatabase( pageCache, dir, config.getRaw() ) )
        {
            if ( !systemDbStoreExists )
            {
                DatabaseInitializer systemDatabaseInitializer = databaseInitializers.apply( SYSTEM_DATABASE_NAME );
                //TODO This all needs to be overhauled
                DatabaseManager<?> databaseManager =
                        ((GraphDatabaseAPI) temporaryDatabase.graphDatabaseService()).getDependencyResolver().resolveDependency( DatabaseManager.class );
                GraphDatabaseFacade systemDatabaseFacade = databaseManager.getDatabaseContext( new DatabaseId( SYSTEM_DATABASE_NAME ) )
                        .orElseThrow( () -> new IllegalStateException( SYSTEM_DATABASE_NAME + " database should exist." ) )
                        .databaseFacade();
                systemDatabaseInitializer.initialize( systemDatabaseFacade );
            }
        }
    }

    private void appendDummyTransactions() throws IOException
    {
        for ( ClusteredDatabaseContext db : clusteredDatabaseManager.registeredDatabases().values() )
        {
            appendNullTransactionLogEntryToSetRaftIndexToMinusOne( db.databaseLayout(), createTemporaryConfig( db ) );
        }
    }

    private Map<String,CoreSnapshot> buildCoreSnapshots( Set<MemberId> members )
    {
        Map<String,CoreSnapshot> snapshots = new HashMap<>();

        RaftCoreState raftCoreState = new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) );
        GlobalSessionTrackerState sessionTrackerState = new GlobalSessionTrackerState();

        for ( ClusteredDatabaseContext db : clusteredDatabaseManager.registeredDatabases().values() )
        {
            CoreSnapshot coreSnapshot = createCoreSnapshot( db, raftCoreState, sessionTrackerState );
            snapshots.put( db.databaseName(), coreSnapshot );
        }

        return snapshots;
    }

    private CoreSnapshot createCoreSnapshot( ClusteredDatabaseContext db, RaftCoreState raftCoreState, GlobalSessionTrackerState sessionTrackerState )
    {
        CoreSnapshot coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, raftCoreState );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTrackerState );
        IdAllocationState idAllocation = deriveIdAllocationState( db.databaseLayout() );
        coreSnapshot.add( CoreStateFiles.ID_ALLOCATION, idAllocation );
        coreSnapshot.add( CoreStateFiles.LOCK_TOKEN, ReplicatedLockTokenState.INITIAL_LOCK_TOKEN );
        return coreSnapshot;
    }

    private Config createTemporaryConfig( ClusteredDatabaseContext db )
    {
        String dbName = db.databaseName();

        Map<String,String> params = new HashMap<>();

        // We want to only inherit things that will affect the storage as necessary during bootstrap of the database
        params.put( default_database.name(), dbName );
        params.put( transaction_logs_root_path.name(), config.get( transaction_logs_root_path ).getAbsolutePath() );

        // For system database default store format will be used, not the configured one
        if ( !dbName.equals( SYSTEM_DATABASE_NAME ) )
        {
            params.put( record_format.name(), config.get( record_format ) );
        }

        return Config.defaults( params );
    }

    private boolean isStorePresent( ClusteredDatabaseContext db )
    {
        return storageEngineFactory.storageExists( fs, pageCache, db.databaseLayout() );
    }

    private void ensureRecoveredOrThrow( DatabaseLayout databaseLayout, Config config ) throws Exception
    {
        if ( Recovery.isRecoveryRequired( fs, databaseLayout, config ) )
        {
            String message = "Cannot bootstrap. Recovery is required. Please ensure that the store being seeded comes from a cleanly shutdown " +
                    "instance of Neo4j or a Neo4j backup";
            log.error( message );
            throw new IllegalStateException( message );
        }
    }

    /**
     * For the purpose of idempotent application from Raft log to the transaction log, every entry in the transaction log
     * carries in its header the corresponding Raft log index. At bootstrap time an empty transaction log entry denoting
     * the beginning of time (Raft log index -1) is created. This is used during recovery by the Raft machinery to pick up
     * where it left off. It is also highly useful for debugging.
     */
    private void appendNullTransactionLogEntryToSetRaftIndexToMinusOne( DatabaseLayout databaseLayout, Config config ) throws IOException
    {
        TransactionIdStore readOnlyTransactionIdStore = storageEngineFactory.readOnlyTransactionIdStore( pageCache, databaseLayout );
        LogFiles logFiles = LogFilesBuilder
                .activeFilesBuilder( databaseLayout, fs, pageCache )
                .withConfig( config )
                .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                .build();

        long dummyTransactionId;
        LogPositionMarker logPositionMarker = new LogPositionMarker();
        try ( Lifespan ignored = new Lifespan( logFiles ) )
        {
            FlushablePositionAwareChannel channel = logFiles.getLogFile().getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );

            long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
            byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
            tx.setHeader( txHeaderBytes, -1, -1, -1, lastCommittedTransactionId, -1, -1 );

            dummyTransactionId = lastCommittedTransactionId + 1;
            channel.getCurrentPosition( logPositionMarker );
            writer.append( tx, dummyTransactionId );
            channel.prepareForFlush().flush();
        }

        try ( TransactionMetaDataStore transactionMetaDataStore = storageEngineFactory.transactionMetaDataStore( fs, databaseLayout, config, pageCache ) )
        {
            transactionMetaDataStore.setLastCommittedAndClosedTransactionId( dummyTransactionId, 0, currentTimeMillis(),
                    logPositionMarker.getByteOffset(), logPositionMarker.getLogVersion() );
        }
    }

    /**
     * In a cluster the ID allocations are based on consensus (rafted). This looks at the store (seed or empty) and
     * figures out what the initial bootstrapped state is.
     */
    private IdAllocationState deriveIdAllocationState( DatabaseLayout databaseLayout )
    {
        DefaultIdGeneratorFactory factory = new DefaultIdGeneratorFactory( fs );

        long[] highIds =
                new long[]{
                        getHighId( factory, NODE, databaseLayout.idNodeStore() ),
                        getHighId( factory, RELATIONSHIP, databaseLayout.idRelationshipStore() ),
                        getHighId( factory, PROPERTY, databaseLayout.idPropertyStore() ),
                        getHighId( factory, STRING_BLOCK, databaseLayout.idPropertyStringStore() ),
                        getHighId( factory, ARRAY_BLOCK, databaseLayout.idPropertyArrayStore() ),
                        getHighId( factory, PROPERTY_KEY_TOKEN, databaseLayout.idPropertyKeyTokenStore() ),
                        getHighId( factory, PROPERTY_KEY_TOKEN_NAME, databaseLayout.idPropertyKeyTokenNamesStore() ),
                        getHighId( factory, RELATIONSHIP_TYPE_TOKEN, databaseLayout.idRelationshipTypeTokenStore() ),
                        getHighId( factory, RELATIONSHIP_TYPE_TOKEN_NAME, databaseLayout.idRelationshipTypeTokenNamesStore() ),
                        getHighId( factory, LABEL_TOKEN, databaseLayout.idLabelTokenStore() ),
                        getHighId( factory, LABEL_TOKEN_NAME, databaseLayout.idLabelTokenNamesStore() ),
                        getHighId( factory, NEOSTORE_BLOCK, databaseLayout.idMetadataStore() ),
                        getHighId( factory, SCHEMA, databaseLayout.idSchemaStore() ),
                        getHighId( factory, NODE_LABELS, databaseLayout.idNodeLabelStore() ),
                        getHighId( factory, RELATIONSHIP_GROUP, databaseLayout.idRelationshipGroupStore() )};

        return new IdAllocationState( highIds, FIRST_INDEX );
    }

    private static long getHighId( DefaultIdGeneratorFactory factory, IdType idType, File idFile )
    {
        IdGenerator idGenerator = factory.open( idFile, idType, () -> -1L, Long.MAX_VALUE );
        long highId = idGenerator.getHighId();
        idGenerator.close();
        return highId;
    }
}
