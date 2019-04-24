/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.SessionTracker;
import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.ReplicationModule;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.TokenRegistry;
import org.neo4j.token.api.TokenHolder;

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

public class CoreStateService implements CoreStateRepository, CoreStateFactory
{
    private final IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final SessionTracker sessionTracker;
    private final StateStorage<Long> lastFlushedStorage;
    private final BooleanSupplier idReuse;
    private final IdContextFactory idContextFactory;
    private final Map<DatabaseId,DatabaseCoreStateComponents> dbStateMap;
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
            RaftMachine raftMachine, ClusteredDatabaseManager databaseManager, ReplicationModule replicationModule,
            StateStorage<Long> lastFlushedStorage, Panicker panicker )
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

        dbStateMap = new ConcurrentHashMap<>();
        dispatchers = new AggregateStateMachinesCommandDispatcher( databaseManager, this );

        idTypeConfigurationProvider = new CommercialIdTypeConfigurationProvider( config );
        idReuse = new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
        Function<DatabaseId,IdGeneratorFactory> idGeneratorProvider =
                databaseId -> createIdGeneratorFactory( fs, logProvider, idTypeConfigurationProvider, databaseId );
        idContextFactory = IdContextFactoryBuilder.of( idTypeConfigurationProvider, globalModule.getJobScheduler() )
                .withIdGenerationFactoryProvider( idGeneratorProvider )
                .withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, idReuse ) ).build();
    }

    @Override
    public DatabaseCoreStateComponents create( DatabaseId databaseId, DatabaseCoreStateComponents.LifecycleDependencies dependencies )
    {
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = createIdAllocationStateMachine( databaseId );

        Supplier<StorageEngine> storageEngineSupplier = dependencies::storageEngine;

        ReplicatedIdRangeAcquirer idRangeAcquirer = new ReplicatedIdRangeAcquirer( databaseId, replicator, idAllocationStateMachine, allocationSizes,
                myself, logProvider );
        DatabaseIdContext idContext = idContextFactory.createIdContext( databaseId );

        TokenRegistry relationshipTypeTokenRegistry = new TokenRegistry( TokenHolder.TYPE_RELATIONSHIP_TYPE );
        ReplicatedRelationshipTypeTokenHolder relationshipTypeTokenHolder = new ReplicatedRelationshipTypeTokenHolder( databaseId,
                relationshipTypeTokenRegistry, replicator, idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry propertyKeyTokenRegistry = new TokenRegistry( TokenHolder.TYPE_PROPERTY_KEY );
        ReplicatedPropertyKeyTokenHolder propertyKeyTokenHolder = new ReplicatedPropertyKeyTokenHolder( databaseId, propertyKeyTokenRegistry, replicator,
                        idContext.getIdGeneratorFactory(), storageEngineSupplier );

        TokenRegistry labelTokenRegistry = new TokenRegistry( TokenHolder.TYPE_LABEL );
        ReplicatedLabelTokenHolder labelTokenHolder = new ReplicatedLabelTokenHolder( databaseId, labelTokenRegistry, replicator,
                idContext.getIdGeneratorFactory(), storageEngineSupplier );

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine = createLockTokenStateMachine( databaseId );

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
                replicatedLockTokenStateMachine, databaseId );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( dependencies::txIdStore, dependencies::txStore, logProvider );

        CoreStateMachines coreStateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relationshipTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLockTokenStateMachine, idAllocationStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        CommitProcessFactory commitProcessFactory = new CoreCommitProcessFactory( databaseId, replicator, coreStateMachines, panicker );

        DatabaseCoreStateComponents dbState = new DatabaseCoreStateComponents( commitProcessFactory, coreStateMachines, tokenHolders,
                idRangeAcquirer, lockManager, idContext );
        dbStateMap.put( databaseId, dbState );
        return dbState;
    }

    private ReplicatedIdAllocationStateMachine createIdAllocationStateMachine( DatabaseId databaseId )
    {
        StateStorage<IdAllocationState> idAllocationStorage = storageFactory.createIdAllocationStorage( databaseId, globalModule.getGlobalLife() );
        return new ReplicatedIdAllocationStateMachine( idAllocationStorage );
    }

    private ReplicatedLockTokenStateMachine createLockTokenStateMachine( DatabaseId databaseId )
    {
        StateStorage<ReplicatedLockTokenState> lockTokenStorage = storageFactory.createLockTokenStorage( databaseId, globalModule.getGlobalLife() );
        return new ReplicatedLockTokenStateMachine( lockTokenStorage );
    }

    public void remove( DatabaseId databaseId )
    {
        dbStateMap.remove( databaseId );
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
            IdTypeConfigurationProvider idTypeConfigurationProvider, DatabaseId databaseId )
    {
        Function<DatabaseId,ReplicatedIdRangeAcquirer> rangeAcquirerFn =
                dId -> getDatabaseState( dId )
                        .map( DatabaseCoreStateComponents::rangeAcquirer )
                        .orElseThrow( () -> new IllegalStateException( String.format( "There is no state found for the database %s", databaseId.name() ) ) );

        return new ReplicatedIdGeneratorFactory( fileSystem, rangeAcquirerFn, logProvider, idTypeConfigurationProvider, databaseId, panicker );
    }

    private Locks createLockManager( final Config config, Clock clock, final LogService logging,
                                     final Replicator replicator, MemberId myself, LeaderLocator leaderLocator,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine, DatabaseId databaseId )
    {
        LocksFactory lockFactory = createLockFactory( config, logging );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, lockTokenStateMachine, databaseId );
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
    public void augmentSnapshot( DatabaseId databaseId, CoreSnapshot coreSnapshot )
    {
        dbStateMap.get( databaseId ).stateMachines().augmentSnapshot( coreSnapshot );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTracker.snapshot() );
    }

    @Override
    public void installSnapshotForDatabase( DatabaseId databaseId, CoreSnapshot coreSnapshot )
    {
        dbStateMap.get( databaseId ).stateMachines().installSnapshot( coreSnapshot );
        // sessionTracker.installSnapshot( coreSnapshot.get( CoreStateFiles.SESSION_TRACKER ) ); // Temporary until we have separate raft groups per database
    }

    @Override
    public void installSnapshotForRaftGroup( CoreSnapshot coreSnapshot )
    {
        sessionTracker.installSnapshot( coreSnapshot.get( CoreStateFiles.SESSION_TRACKER ) );
    }

    @Override
    public void flush( long lastApplied ) throws IOException
    {
        for ( DatabaseCoreStateComponents db : dbStateMap.values() )
        {
            db.stateMachines().flush();
        }
        sessionTracker.flush();
        lastFlushedStorage.writeState( lastApplied );
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
    public Map<DatabaseId,DatabaseCoreStateComponents> getAllDatabaseStates()
    {
        return dbStateMap;
    }

    @Override
    public Optional<DatabaseCoreStateComponents> getDatabaseState( DatabaseId databaseId )
    {
        return Optional.ofNullable( dbStateMap.get( databaseId ) );
    }

    @Override
    public long getLastFlushed()
    {
        return lastFlushedStorage.getInitialState();
    }
}
