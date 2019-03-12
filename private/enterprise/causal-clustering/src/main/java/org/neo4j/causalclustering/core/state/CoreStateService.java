/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.ReplicationModule;
import org.neo4j.causalclustering.SessionTracker;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.CoreLocalDatabase;
import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.replication.Replicator;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyMachine;
import org.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;
import org.neo4j.causalclustering.core.state.machines.id.FreeIdFilteredIdGeneratorFactory;
import org.neo4j.causalclustering.core.state.machines.id.IdReusabilityCondition;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationStateMachine;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdGeneratorFactory;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdRangeAcquirer;
import org.neo4j.causalclustering.core.state.machines.locks.LeaderOnlyLockManager;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenStateMachine;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedLabelTokenHolder;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedPropertyKeyTokenHolder;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedRelationshipTypeTokenHolder;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenStateMachine;
import org.neo4j.causalclustering.core.state.machines.tx.RecoverConsensusLogIndex;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionCommitProcess;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.EditionLocksFactories;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.core.TokenRegistry;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.StorageEngine;

import static java.lang.Long.max;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.array_block_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.label_token_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.label_token_name_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.neostore_block_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.node_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.node_labels_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.property_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.property_key_token_name_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.relationship_group_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.relationship_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.relationship_type_token_name_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.schema_id_allocation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_apply_max_batch_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.string_block_id_allocation_size;
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
    private final CoreStateStorageService storage;
    private final MemberId myself;
    private final Map<IdType,Integer> allocationSizes;
    private final LogService logging;
    private final LogProvider logProvider;
    private final CommandIndexTracker commandIndexTracker;
    private final VersionContextSupplier versionContextSupplier;
    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final FileSystemAbstraction fs;
    private final PlatformModule platform;
    private final Config config;
    private final RaftMachine raftMachine;
    private final AggregateStateMachinesCommandDispatcher dispatchers;

    public CoreStateService( MemberId myself, PlatformModule platformModule, CoreStateStorageService storage, Config config, RaftMachine raftMachine,
            DatabaseService databaseService, ReplicationModule replicationModule, StateStorage<Long> lastFlushedStorage )
    {
        this.logging = platformModule.logging;
        this.logProvider = logging.getInternalLogProvider();
        this.lastFlushedStorage = lastFlushedStorage;
        this.storage = storage;
        this.raftMachine = raftMachine;
        this.replicator = replicationModule.getReplicator();
        this.myself = myself;
        this.config = config;

        platform = platformModule;
        fs = platformModule.fileSystem;
        sessionTracker = replicationModule.getSessionTracker();
        allocationSizes = getIdTypeAllocationSizeFromConfig( config );
        commandIndexTracker = platformModule.dependencies.satisfyDependency( new CommandIndexTracker() );

        versionContextSupplier = platformModule.versionContextSupplier;
        cursorTracerSupplier = platformModule.tracers.pageCursorTracerSupplier;

        dbStateMap = new CopyOnWriteHashMap<>();
        dispatchers = new AggregateStateMachinesCommandDispatcher( databaseService, this );

        idTypeConfigurationProvider = new EnterpriseIdTypeConfigurationProvider( config );
        idReuse = new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
        Function<String,IdGeneratorFactory> idGeneratorProvider =
                databaseName -> createIdGeneratorFactory( fs, logProvider, idTypeConfigurationProvider, databaseName );
        idContextFactory = IdContextFactoryBuilder.of( idTypeConfigurationProvider, platformModule.jobScheduler )
                .withIdGenerationFactoryProvider( idGeneratorProvider )
                .withFactoryWrapper( generator -> new FreeIdFilteredIdGeneratorFactory( generator, idReuse ) ).build();
    }

    public PerDatabaseCoreStateComponents create( CoreLocalDatabase localDatabase )
    {
        String databaseName = localDatabase.databaseName();
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine(
                storage.stateStorage( CoreStateFiles.ID_ALLOCATION, databaseName ) );

        Supplier<StorageEngine> storageEngineSupplier = () -> localDatabase.dataSource().getDependencyResolver().resolveDependency( StorageEngine.class );

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

        ReplicatedLockTokenStateMachine replicatedLockTokenStateMachine =
                new ReplicatedLockTokenStateMachine( storage.stateStorage( CoreStateFiles.LOCK_TOKEN, databaseName ) );

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

        Locks lockManager = createLockManager( config, platform.clock, logging, replicator, myself, raftMachine,
                replicatedLockTokenStateMachine, databaseName );

        RecoverConsensusLogIndex consensusLogIndexRecovery = new RecoverConsensusLogIndex( localDatabase, logProvider );

        CoreStateMachines coreStateMachines = new CoreStateMachines( replicatedTxStateMachine, labelTokenStateMachine, relationshipTypeTokenStateMachine,
                propertyKeyTokenStateMachine, replicatedLockTokenStateMachine, idAllocationStateMachine, new DummyMachine(), consensusLogIndexRecovery );

        TokenHolders tokenHolders = new TokenHolders( propertyKeyTokenHolder, labelTokenHolder, relationshipTypeTokenHolder );

        CommitProcessFactory commitProcessFactory = ( appender, applier, ignored ) -> createCommitProcess( appender, applier, localDatabase );

        PerDatabaseCoreStateComponents dbState = new PerDatabaseCoreStateComponents( commitProcessFactory, coreStateMachines, tokenHolders,
                idRangeAcquirer, lockManager, idContext );
        localDatabase.setCoreStateComponents( dbState );
        dbStateMap.put( databaseName, dbState );
        return dbState;
    }

    private TransactionCommitProcess createCommitProcess( TransactionAppender appender, StorageEngine storageEngine, CoreLocalDatabase localDatabase )
    {
        localDatabase.setCommitProcessDependencies( appender, storageEngine );
        return new ReplicatedTransactionCommitProcess( replicator, localDatabase.databaseName() );
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

        return new ReplicatedIdGeneratorFactory( fileSystem, rangeAcquirerFn, logProvider, idTypeConfigurationProvider, databaseName );
    }

    private Locks createLockManager( final Config config, Clock clock, final LogService logging,
                                     final Replicator replicator, MemberId myself, LeaderLocator leaderLocator,
                                     ReplicatedLockTokenStateMachine lockTokenStateMachine, String databaseName )
    {
        LocksFactory lockFactory = createLockFactory( config, logging );
        Locks localLocks = EditionLocksFactories.createLockManager( lockFactory, config, clock );
        return new LeaderOnlyLockManager( myself, replicator, leaderLocator, localLocks, lockTokenStateMachine, databaseName );
    }

    @Override
    public void augmentSnapshot( CoreSnapshot coreSnapshot )
    {
        dbStateMap.forEach( ( dbName, dbState ) -> dbState.stateMachines().addSnapshots( dbName, coreSnapshot ) );
        coreSnapshot.add( CoreStateFiles.SESSION_TRACKER, sessionTracker.snapshot() );
    }

    @Override
    public void installSnapshot( CoreSnapshot coreSnapshot )
    {
        dbStateMap.forEach( ( dbName, dbState ) -> dbState.stateMachines().installSnapshots( dbName, coreSnapshot ) );
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
