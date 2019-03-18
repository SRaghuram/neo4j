/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.ReplicationModule;
import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.core.CoreLocalDatabase;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import com.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.id.IdReusabilityCondition;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdGeneratorFactory;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdRangeAcquirer;
import com.neo4j.causalclustering.core.state.machines.locks.LeaderOnlyLockManager;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedLabelTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedPropertyKeyTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedRelationshipTypeTokenHolder;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import com.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionCommitProcess;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.monitoring.ThroughputMonitor;
import com.neo4j.kernel.impl.enterprise.id.CommercialIdTypeConfigurationProvider;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.common.CopyOnWriteHashMap;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StorageEngine;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.array_block_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.label_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.label_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.neostore_block_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.node_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.node_labels_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_group_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_name_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.schema_id_allocation_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.status_throughput_window;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.string_block_id_allocation_size;
import static java.lang.Long.max;
import static org.neo4j.graphdb.factory.EditionLocksFactories.createLockFactory;

public class CoreStateService implements CoreStateRepository, CoreStateFactory<CoreLocalDatabase>
{
    private final IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final SessionTracker sessionTracker;
    private final StateStorage<Long> lastFlushedStorage;
    private final BooleanSupplier idReuse;
    private final IdContextFactory idContextFactory;
    private final Map<String,PerDatabaseCoreStateComponents> dbStateMap;
    private final Replicator replicator;
    private final CoreStateStorageFactory storageFactory;
    private final MemberId myself;
    private final Map<IdType,Integer> allocationSizes;
    private final LogService logging;
    private final Panicker panicker;
    private final LogProvider logProvider;
    private final CommandIndexTracker commandIndexTracker;
    private final VersionContextSupplier versionContextSupplier;
    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final FileSystemAbstraction fs;
    private final GlobalModule globalModule;
    private final Config config;
    private final RaftMachine raftMachine;
    private final AggregateStateMachinesCommandDispatcher dispatchers;

    public CoreStateService( MemberId myself, GlobalModule globalModule, CoreStateStorageFactory storageFactory, Config config,
            RaftMachine raftMachine, DatabaseService databaseService, ReplicationModule replicationModule, StateStorage<Long> lastFlushedStorage,
            Panicker panicker )
    {
        this.logging = globalModule.getLogService();
        this.panicker = panicker;
        this.logProvider = logging.getInternalLogProvider();
        this.lastFlushedStorage = lastFlushedStorage;
        this.storageFactory = storageFactory;
        this.raftMachine = raftMachine;
        this.replicator = replicationModule.getReplicator();
        this.myself = myself;
        this.config = config;
        this.globalModule = globalModule;

        fs = globalModule.getFileSystem();
        sessionTracker = replicationModule.getSessionTracker();
        allocationSizes = getIdTypeAllocationSizeFromConfig( config );
        commandIndexTracker = globalModule.getGlobalDependencies().satisfyDependency( new CommandIndexTracker() );
        initialiseStatusDescriptionEndpoint( globalModule, commandIndexTracker );

        versionContextSupplier = globalModule.getVersionContextSupplier();
        cursorTracerSupplier = globalModule.getTracers().getPageCursorTracerSupplier();

        dbStateMap = new CopyOnWriteHashMap<>();
        dispatchers = new AggregateStateMachinesCommandDispatcher( databaseService, this );

        idTypeConfigurationProvider = new CommercialIdTypeConfigurationProvider( config );
        idReuse = new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
        Function<String,IdGeneratorFactory> idGeneratorProvider =
                databaseName -> createIdGeneratorFactory( fs, logProvider, idTypeConfigurationProvider, databaseName );
        idContextFactory = IdContextFactoryBuilder.of( idTypeConfigurationProvider, globalModule.getJobScheduler() )
                .withIdGenerationFactoryProvider( idGeneratorProvider )
                .withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, idReuse ) ).build();
    }

    public PerDatabaseCoreStateComponents create( CoreLocalDatabase localDatabase )
    {
        String databaseName = localDatabase.databaseName();
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = createIdAllocationStateMachine( databaseName );

        Supplier<StorageEngine> storageEngineSupplier = () -> localDatabase.database().getDependencyResolver().resolveDependency( StorageEngine.class );

        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( databaseName, replicator, idAllocationStateMachine, allocationSizes,
                myself, logProvider );
        DatabaseIdContext idContext = idContextFactory.createIdContext( databaseName );

        TokenRegistry relationshipTypeTokenRegistry = new TokenRegistry( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder( databaseName,
                relationshipTypeTokenRegistry, replicator, idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry propertyKeyTokenRegistry = new TokenRegistry( TokenHolder.TYPE_PROPERTY_KEY );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder( databaseName, propertyKeyTokenRegistry, replicator,
                        idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry labelTokenRegistry = new TokenRegistry( TokenHolder.TYPE_LABEL );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder( databaseName, labelTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier );

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine = createLockTokenStateMachine( databaseName );

        ReplicatedTokenStateMachine labelTokenStateMachine = new ReplicatedTokenStateMachine( labelTokenRegistry,
                logProvider, versionContextSupplier );
        ReplicatedTokenStateMachine propertyKeyTokenStateMachine =
                new ReplicatedTokenStateMachine( propertyKeyTokenRegistry, logProvider, versionContextSupplier );

        ReplicatedTokenStateMachine relationshipTypeTokenStateMachine =
                new ReplicatedTokenStateMachine( relationshipTypeTokenRegistry, logProvider, versionContextSupplier );

        ReplicatedTransactionStateMachine replicatedTxStateMachine =
                new ReplicatedTransactionStateMachine( commandIndexTracker, replicatedLockTokenStateMachine,
                        config.get( state_machine_apply_max_batch_size ), logProvider, cursorTracerSupplier,
                        versionContextSupplier );

        Locks lockManager = createLockManager( config, globalModule.getGlobalClock(), logging, replicator, myself, raftMachine,
                replicatedLockTokenStateMachine, databaseName );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( localDatabase, logProvider );

        CoreStateMachines coreStateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relationshipTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLockTokenStateMachine, idAllocationStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        CommitProcessFactory commitProcessFactory = ( appender, applier, ignored ) -> createCommitProcess( appender, applier, localDatabase, panicker );

        PerDatabaseCoreStateComponents dbState = new PerDatabaseCoreStateComponents( commitProcessFactory, coreStateMachines, tokenHolders,
                idRangeAcquirer, lockManager, idContext );
        localDatabase.setCoreStateComponents( dbState );
        dbStateMap.put( databaseName, dbState );
        return dbState;
    }

    private ReplicatedIdAllocationStateMachine createIdAllocationStateMachine( String databaseName )
    {
        StateStorage<IdAllocationState> idAllocationStorage = storageFactory.createIdAllocationStorage( databaseName, globalModule.getGlobalLife() );
        return new ReplicatedIdAllocationStateMachine( idAllocationStorage );
    }

    private ReplicatedLockTokenStateMachine createLockTokenStateMachine( String databaseName )
    {
        StateStorage<ReplicatedLockTokenState> lockTokenStorage = storageFactory.createLockTokenStorage( databaseName, globalModule.getGlobalLife() );
        return new ReplicatedLockTokenStateMachine( lockTokenStorage );
    }

    private TransactionCommitProcess createCommitProcess( TransactionAppender appender, StorageEngine storageEngine, CoreLocalDatabase localDatabase,
            Panicker panicker )
    {
        localDatabase.setCommitProcessDependencies( appender, storageEngine );
        return new ReplicatedTransactionCommitProcess( replicator, localDatabase.databaseName(), panicker );
    }

    private Map<IdType,Integer> getIdTypeAllocationSizeFromConfig( Config config )
    {
        Map<IdType,Integer> allocationSizes = new HashMap<>( IdType.values().length );
        allocationSizes.put( IdType.NODE, config.get( node_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP, config.get( relationship_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY, config.get( property_id_allocation_size ) );
        allocationSizes.put( IdType.STRING_BLOCK, config.get( string_block_id_allocation_size ) );
        allocationSizes.put( IdType.ARRAY_BLOCK, config.get( array_block_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN, config.get( property_key_token_id_allocation_size ) );
        allocationSizes.put( IdType.PROPERTY_KEY_TOKEN_NAME, config.get( property_key_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN, config.get( relationship_type_token_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_TYPE_TOKEN_NAME, config.get( relationship_type_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN, config.get( label_token_id_allocation_size ) );
        allocationSizes.put( IdType.LABEL_TOKEN_NAME, config.get( label_token_name_id_allocation_size ) );
        allocationSizes.put( IdType.NEOSTORE_BLOCK, config.get( neostore_block_id_allocation_size ) );
        allocationSizes.put( IdType.SCHEMA, config.get( schema_id_allocation_size ) );
        allocationSizes.put( IdType.NODE_LABELS, config.get( node_labels_id_allocation_size ) );
        allocationSizes.put( IdType.RELATIONSHIP_GROUP, config.get( relationship_group_id_allocation_size ) );
        return allocationSizes;
    }

    private IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fileSystem, final LogProvider logProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider, String databaseName )
    {
        Function<String,ReplicatedIdRangeAcquirer> rangeAcquirerFn =
                dbName -> getDatabaseState( dbName )
                        .map( PerDatabaseCoreStateComponents::rangeAcquirer )
                        .orElseThrow( () -> new IllegalStateException( String.format( "There is no state found for the database %s", databaseName ) ) );

        return new ReplicatedIdGeneratorFactory( fileSystem, rangeAcquirerFn, logProvider, idTypeConfigurationProvider, databaseName, panicker );
    }

    private Locks createLockManager( final Config config, Clock clock, final LogService logging,
                                     final Replicator replicator, MemberId myself, LeaderLocator leaderLocator,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine, String databaseName )
    {
        LocksFactory lockFactory = createLockFactory( config, logging );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, lockTokenStateMachine, databaseName );
    }

    private void initialiseStatusDescriptionEndpoint( GlobalModule globalModule, CommandIndexTracker commandIndexTracker )
    {
        Duration samplingWindow = config.get( status_throughput_window );
        ThroughputMonitor throughputMonitor = new ThroughputMonitor( logProvider, globalModule.getGlobalClock(), globalModule.getJobScheduler(), samplingWindow,
                commandIndexTracker::getAppliedCommandIndex );
        globalModule.getGlobalLife().add( throughputMonitor );
        globalModule.getGlobalDependencies().satisfyDependency( throughputMonitor );
    }

    @Override
    public void augmentSnapshot( String databaseName, CoreSnapshot coreSnapshot )
    {
        dbStateMap.get( databaseName ).stateMachines().augmentSnapshot( coreSnapshot );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTracker.snapshot() );
    }

    @Override
    public void installSnapshot( String databaseName, CoreSnapshot coreSnapshot )
    {
        dbStateMap.get( databaseName ).stateMachines().installSnapshot( coreSnapshot );
        sessionTracker.installSnapshot( coreSnapshot.get( CoreStateFiles.SESSION_TRACKER ) );
    }

    @Override
    public void flush( long lastApplied ) throws IOException
    {
        for ( PerDatabaseCoreStateComponents db : dbStateMap.values() )
        {
            db.stateMachines().flush();
        }
        sessionTracker.flush();
        lastFlushedStorage.persistStoreData( lastApplied );
    }

    @Override
    public CommandDispatcher commandDispatcher()
    {
        return dispatchers;
    }

    @Override
    public long getLastAppliedIndex()
    {
        Optional<Long> maxFromStateMachines = dbStateMap.values().stream()
                .map( db -> db.stateMachines().getLastAppliedIndex() )
                .max( Long::compareTo );
        long maxFromSession = sessionTracker.getLastAppliedIndex();
        return maxFromStateMachines.map( m -> max( m, maxFromSession ) ).orElse( maxFromSession );
    }

    @Override
    public Map<String,PerDatabaseCoreStateComponents> getAllDatabaseStates()
    {
        return dbStateMap;
    }

    @Override
    public Optional<PerDatabaseCoreStateComponents> getDatabaseState( String databaseName )
    {
        return Optional.ofNullable( dbStateMap.get( databaseName ) );
    }

    @Override
    public long getLastFlushed()
    {
        return lastFlushedStorage.getInitialState();
    }
}
