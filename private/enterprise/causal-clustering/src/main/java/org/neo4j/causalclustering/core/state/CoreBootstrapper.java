/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.machines.tx.LogIndexTxHeaderEncoding;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.helper.TemporaryDatabase;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.DatabaseInitializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.recovery.RecoveryRequiredException;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.active_database;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.ignore_store_lock;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_logs_location;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.kernel.impl.recovery.RecoveryRequiredChecker.assertRecoveryIsNotRequired;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.id.IdType.ARRAY_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.NEOSTORE_BLOCK;
import static org.neo4j.kernel.impl.store.id.IdType.NODE;
import static org.neo4j.kernel.impl.store.id.IdType.NODE_LABELS;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.id.IdType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.id.IdType.SCHEMA;
import static org.neo4j.kernel.impl.store.id.IdType.STRING_BLOCK;

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

    private final DatabaseService databaseService;
    private final TemporaryDatabase.Factory tempDatabaseFactory;
    private final Function<String,DatabaseInitializer> databaseInitializers;
    private final PageCache pageCache;
    private final FileSystemAbstraction fs;
    private final Config activeDatabaseConfig;
    private final LogProvider logProvider;
    private final Log log;

    CoreBootstrapper( DatabaseService databaseService, TemporaryDatabase.Factory tempDatabaseFactory, Function<String,DatabaseInitializer> databaseInitializers,
            FileSystemAbstraction fs, Config activeDatabaseConfig, LogProvider logProvider, PageCache pageCache )
    {
        this.databaseService = databaseService;
        this.tempDatabaseFactory = tempDatabaseFactory;
        this.databaseInitializers = databaseInitializers;
        this.fs = fs;
        this.activeDatabaseConfig = activeDatabaseConfig;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Bootstraps the cluster using the supplied set of members.
     *
     * @param members the members to bootstrap with (this comes from the discovery service).
     * @return a snapshot which represents the initial state.
     * @throws IOException if an I/O exception occurs.
     */
    public CoreSnapshot bootstrap( Set<MemberId> members ) throws IOException, RecoveryRequiredException
    {
        int databaseCount = databaseService.registeredDatabases().size();
        if ( databaseCount != 1 && databaseCount != 2 )
        {
            throw new IllegalArgumentException( "Not supporting " + databaseCount + " number of databases." );
        }

        String activeDatabaseName = activeDatabaseConfig.get( GraphDatabaseSettings.active_database );
        LocalDatabase activeDatabase = getDatabaseOrThrow( activeDatabaseName );

        LocalDatabase systemDatabase = null;
        if ( databaseCount == 2 )
        {
            systemDatabase = getDatabaseOrThrow( SYSTEM_DATABASE_NAME );
        }

        CoreSnapshot coreSnapshot = new CoreSnapshot( FIRST_INDEX, FIRST_TERM );
        coreSnapshot.add( CoreStateFiles.RAFT_CORE_STATE, new RaftCoreState( new MembershipEntry( FIRST_INDEX, members ) ) );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, new GlobalSessionTrackerState() );

        bootstrapDatabase( activeDatabase, activeDatabaseConfig, null );
        appendNullTransactionLogEntryToSetRaftIndexToMinusOne( activeDatabase.databaseLayout(), activeDatabaseConfig );

        IdAllocationState activeDatabaseIdAllocationState = deriveIdAllocationState( activeDatabase.databaseLayout() );
        coreSnapshot.add( activeDatabaseName, CoreStateFiles.ID_ALLOCATION, activeDatabaseIdAllocationState );
        coreSnapshot.add( activeDatabaseName, CoreStateFiles.LOCK_TOKEN, ReplicatedLockTokenState.INITIAL_LOCK_TOKEN );

        if ( databaseCount == 2 )
        {
            DatabaseInitializer systemDatabaseInitializer = databaseInitializers.apply( SYSTEM_DATABASE_NAME );
            bootstrapDatabase( systemDatabase, Config.defaults(), systemDatabaseInitializer );
            appendNullTransactionLogEntryToSetRaftIndexToMinusOne( systemDatabase.databaseLayout(), Config.defaults() );

            IdAllocationState systemDatabaseIdAllocationState = deriveIdAllocationState( systemDatabase.databaseLayout() );
            coreSnapshot.add( SYSTEM_DATABASE_NAME, CoreStateFiles.ID_ALLOCATION, systemDatabaseIdAllocationState );
            coreSnapshot.add( SYSTEM_DATABASE_NAME, CoreStateFiles.LOCK_TOKEN, ReplicatedLockTokenState.INITIAL_LOCK_TOKEN );
        }

        return coreSnapshot;
    }

    private Map<String,String> initializerParams( Config databaseConfig )
    {
        Map<String,String> params = new HashMap<>();

        /* This adhoc inheritance of configuration options is unfortunate and fragile, but there really aren't any better options currently. */
        params.put( GraphDatabaseSettings.record_format.name(), databaseConfig.get( record_format ) );
        params.put( GraphDatabaseSettings.logical_logs_location.name(), databaseConfig.get( logical_logs_location ).getAbsolutePath() );
        params.put( GraphDatabaseSettings.active_database.name(), databaseConfig.get( active_database ) );

        /* This adhoc quiescing of services is unfortunate and fragile, but there really aren't any better options currently. */
        params.put( GraphDatabaseSettings.pagecache_warmup_enabled.name(), FALSE );
        params.put( OnlineBackupSettings.online_backup_enabled.name(), FALSE );

        /* Touching the store is allowed during bootstrapping. */
        params.put( ignore_store_lock.name(), TRUE );

        return params;
    }

    void bootstrapDatabase( LocalDatabase database, Config databaseConfig, DatabaseInitializer databaseInitializer )
            throws IOException, RecoveryRequiredException
    {
        DatabaseLayout databaseLayout = database.databaseLayout();
        ensureRecoveredOrThrow( databaseLayout, databaseConfig );

        if ( NeoStores.isStorePresent( pageCache, databaseLayout ) )
        {
            return;
        }

        initializeDatabase( databaseLayout.databaseDirectory(), databaseConfig, databaseInitializer );
    }

    private LocalDatabase getDatabaseOrThrow( String databaseName )
    {
        LocalDatabase localDatabase = databaseService.registeredDatabases().get( databaseName );
        if ( localDatabase == null )
        {
            throw new IllegalStateException( "Should have found database named " + databaseName );
        }
        return localDatabase;
    }

    private void ensureRecoveredOrThrow( DatabaseLayout databaseLayout, Config config ) throws IOException, RecoveryRequiredException
    {
        try
        {
            assertRecoveryIsNotRequired( fs, pageCache, config, databaseLayout, new Monitors() );
        }
        catch ( RecoveryRequiredException e )
        {
            log.error( e.getMessage() );
            throw e;
        }
    }

    /**
     * This method has the purpose of both creating a database and optionally initializing its contents.
     */
    private void initializeDatabase( File databaseDirectory, Config databaseConfig, DatabaseInitializer databaseInitializer )
            throws IOException
    {
        Map<String,String> params = initializerParams( databaseConfig );

        try ( TemporaryDatabase temporaryDatabase = tempDatabaseFactory.startTemporaryDatabase( databaseDirectory, params ) )
        {
            if ( databaseInitializer != null )
            {
                databaseInitializer.initialize( temporaryDatabase.graphDatabaseService() );
            }
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
        ReadOnlyTransactionIdStore readOnlyTransactionIdStore = new ReadOnlyTransactionIdStore( pageCache, databaseLayout );
        LogFiles logFiles = LogFilesBuilder
                .activeFilesBuilder( databaseLayout, fs, pageCache )
                .withConfig( config )
                .withLastCommittedTransactionIdSupplier( () -> readOnlyTransactionIdStore.getLastClosedTransactionId() - 1 )
                .build();

        long dummyTransactionId;
        try ( Lifespan ignored = new Lifespan( logFiles ) )
        {
            FlushableChannel channel = logFiles.getLogFile().getWriter();
            TransactionLogWriter writer = new TransactionLogWriter( new LogEntryWriter( channel ) );

            long lastCommittedTransactionId = readOnlyTransactionIdStore.getLastCommittedTransactionId();
            PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( Collections.emptyList() );
            byte[] txHeaderBytes = LogIndexTxHeaderEncoding.encodeLogIndexAsTxHeader( -1 );
            tx.setHeader( txHeaderBytes, -1, -1, -1, lastCommittedTransactionId, -1, -1 );

            dummyTransactionId = lastCommittedTransactionId + 1;
            writer.append( tx, dummyTransactionId );
            channel.prepareForFlush().flush();
        }

        File neoStoreFile = databaseLayout.metadataStore();
        MetaDataStore.setRecord( pageCache, neoStoreFile, LAST_TRANSACTION_ID, dummyTransactionId );
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
