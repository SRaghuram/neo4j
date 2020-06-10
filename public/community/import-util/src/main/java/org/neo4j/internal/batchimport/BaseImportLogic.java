package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.cache.idmapping.reverseIdMap.NodeInputIdPropertyLookup;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionSupervisors;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.BatchingStoreInterface;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactory.auto;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.IOUtils.closeAll;

public abstract class BaseImportLogic implements ImportLogicInterface {
    public static final String NODE_IMPORT_NAME = "Nodes";
    public static final String RELATIONSHIP_IMPORT_NAME = "Relationships";
    public interface Monitor
    {
        void doubleRelationshipRecordUnitsEnabled();

        void mayExceedNodeIdCapacity( long capacity, long estimatedCount );

        void mayExceedRelationshipIdCapacity( long capacity, long estimatedCount );

        void insufficientHeapSize( long optimalMinimalHeapSize, long heapSize );

        void abundantHeapSize( long optimalMinimalHeapSize, long heapSize );

        void insufficientAvailableMemory( long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory );
    }
    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void mayExceedRelationshipIdCapacity( long capacity, long estimatedCount )
        {   // no-op
        }

        @Override
        public void mayExceedNodeIdCapacity( long capacity, long estimatedCount )
        {   // no-op
        }

        @Override
        public void doubleRelationshipRecordUnitsEnabled()
        {   // no-op
        }

        @Override
        public void insufficientHeapSize( long optimalMinimalHeapSize, long heapSize )
        {   // no-op
        }

        @Override
        public void abundantHeapSize( long optimalMinimalHeapSize, long heapSize )
        {   // no-op
        }

        @Override
        public void insufficientAvailableMemory( long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory )
        {   // no-op
        }
    };
    protected static final String IMPORT_COUNT_STORE_REBUILD_TAG = "importCountStoreRebuild";
    protected IdMapper idMapper;
    protected BatchingStoreBase basicNeoStore;
    protected Collector badCollector;
    protected Configuration config;
    protected NumberArrayFactory numberArrayFactory;
    protected DataImporterMonitor storeUpdateMonitor = new DataImporterMonitor();
    protected PageCacheTracer pageCacheTracer;
    protected DatabaseLayout databaseLayout;
    protected ExecutionMonitor executionMonitor;
    protected Log log;
    protected Input input;
    protected Config dbConfig;
    protected NodeRelationshipCache nodeRelationshipCache;
    protected Monitor monitor;
    protected FileSystemAbstraction fileSystem;
    protected PropertyValueLookup inputIdLookup = null;
    protected final MemoryTracker memoryTracker;
    protected NodeLabelsCache nodeLabelsCache;
    protected long startTime;
    // This map contains additional state that gets populated, created and used throughout the stages.
    // The reason that this is a map is to allow for a uniform way of accessing and loading this stage
    // from the outside. Currently these things live here:
    //   - RelationshipTypeDistribution
    protected final Map<Class<?>,Object> accessibleState = new HashMap<>();
    protected final Dependencies dependencies = new Dependencies();
    protected long peakMemoryUsage;
    protected boolean successful;

    public BaseImportLogic(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, BatchingStoreBase neoStore, Collector badCollector, LogService logService, ExecutionMonitor executionMonitor,
                           Configuration config, Config dbConfig, Monitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker)
    {
        this.fileSystem = fileSystem;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.dbConfig = dbConfig;
        this.basicNeoStore = neoStore;
        this.badCollector = badCollector;
        this.config = config;
        this.monitor = monitor;
        this.databaseLayout = databaseLayout;
        this.pageCacheTracer = pageCacheTracer;
        this.executionMonitor = executionMonitor;
        this.memoryTracker = memoryTracker;
    }

    public void initialize( Input input ) throws IOException {
        log.info( "Import starting" );
        startTime = currentTimeMillis();
        this.input = input;
        PageCacheArrayFactoryMonitor numberArrayFactoryMonitor = new PageCacheArrayFactoryMonitor();
        numberArrayFactory = auto( basicNeoStore.getPageCache(), pageCacheTracer, databaseLayout.databaseDirectory(), config.allowCacheAllocationOnHeap(),
                numberArrayFactoryMonitor );
        inputIdLookup = getNodeInputIdLookup();
        // Some temporary caches and indexes in the import
        idMapper = instantiateIdMapper( input );
        nodeRelationshipCache = new NodeRelationshipCache( numberArrayFactory, dbConfig.get( GraphDatabaseSettings.dense_node_threshold ), memoryTracker);
        dependencies.satisfyDependencies( idMapper, basicNeoStore, nodeRelationshipCache, numberArrayFactoryMonitor, BatchingStoreInterface.class);
    }
    /**
     * Prepares {@link IdMapper} to be queried for ID --> nodeId lookups. This is required for running {@link #importRelationships()}.
     */
    @Override
    public void prepareIdMapper()
    {
        if ( idMapper.needsPreparation() )
        {
            MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( basicNeoStore, idMapper );
            executeStage( new IdMapperPreparationStage( config, idMapper, inputIdLookup, badCollector, memoryUsageStats ) );
            final LongIterator duplicateNodeIds = idMapper.leftOverDuplicateNodesIds();
            if ( duplicateNodeIds.hasNext() )
            {
                executeStage( new DeleteDuplicateNodesStage1( config, duplicateNodeIds, basicNeoStore, storeUpdateMonitor, pageCacheTracer ) );
            }
            updatePeakMemoryUsage();
        }
    }

    private IdMapper instantiateIdMapper( Input input )
    {
        PageCacheArrayFactoryMonitor numberArrayFactoryMonitor = new PageCacheArrayFactoryMonitor();
        numberArrayFactory = auto( basicNeoStore.getPageCache(), pageCacheTracer, databaseLayout.databaseDirectory(), config.allowCacheAllocationOnHeap(),
                numberArrayFactoryMonitor );
        switch ( input.idType() )
        {
            case STRING:
                return IdMappers.strings( numberArrayFactory, input.groups(), pageCacheTracer, memoryTracker );
            case INTEGER:
                return IdMappers.longs( numberArrayFactory, input.groups(), pageCacheTracer, memoryTracker );
            case ACTUAL:
                return IdMappers.actual();
            default:
                throw new IllegalArgumentException( "Unsupported id type " + input.idType() );
        }
    }

    protected void executeStage( Stage stage )
    {
        ExecutionSupervisors.superviseExecution( executionMonitor, stage );
    }
    public NumberArrayFactory getNumberArrayFactory()
    {
        return numberArrayFactory;
    }
    @Override
    public PropertyValueLookup getNodeInputIdLookup() {
        if (inputIdLookup != null)
            return inputIdLookup;
        return new NodeInputIdPropertyLookup((DatabaseLayout.ofFlat( databaseLayout.file( "temp" ) )).databaseDirectory(), basicNeoStore.getPageCache() );
    }

    /**
     * Builds the counts store. Requires that {@link #importNodes()} and {@link #importRelationships()} has run.
     */
    /*public void buildCountsStore()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( IMPORT_COUNT_STORE_REBUILD_TAG ) )
        {
            basicNeoStore.buildCountsStore( new CountsBuilder()
            {
                @Override
                public void initialize(CountsAccessor.Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
                {
                    MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
                    nodeLabelsCache = new NodeLabelsCache( numberArrayFactory, basicNeoStore.getLabelRepository().getHighId(), memoryTracker );
                    MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( basicNeoStore, nodeLabelsCache );
                    executeStage( new NodeCountsAndLabelIndexBuildStage( config, nodeLabelsCache, basicNeoStore.getNodeStore(),
                            basicNeoStore.getLabelRepository().getHighId(),
                            updater, progressMonitor.startSection( "Nodes" ), basicNeoStore.getLabelScanStore(), pageCacheTracer, memoryUsageStats ) );
                    // Count label-[type]->label
                    executeStage( new RelationshipCountsAndTypeIndexBuildStage( config, nodeLabelsCache, neoStore.getRelationshipStore(),
                            basicNeoStore.getLabelRepository().getHighId(),
                            basicNeoStore.getRelationshipTypeRepository().getHighId(), updater, numberArrayFactory,
                            progressMonitor.startSection( "Relationships" ), basicNeoStore.getRelationshipTypeScanStore(), pageCacheTracer, memoryTracker ) );
                }

                @Override
                public long lastCommittedTxId()
                {
                    return neoStore.getLastCommittedTransactionId();
                }
            }, pageCacheTracer, cursorTracer, memoryTracker );
        }
    }*/
    /**
     * Accesses state of a certain {@code type}. This is state that may be long- or short-lived and perhaps
     * created in one part of the import to be used in another.
     *
     * @param type {@link Class} of the state to get.
     * @return the state of the given type.
     * @throws IllegalStateException if the state of the given {@code type} isn't available.
     */
    public <T> T getState( Class<T> type )
    {
        return type.cast( accessibleState.get( type ) );
    }

    /**
     * Puts state of a certain type.
     *
     * @param state state instance to set.
     * @see #getState(Class)
     * @throws IllegalStateException if state of this type has already been defined.
     */
    public <T> void putState( T state )
    {
        accessibleState.put( state.getClass(), state );
        dependencies.satisfyDependency( state );
    }
    @Override
    public void close() throws IOException
    {
        // We're done, do some final logging about it
        long totalTimeMillis = currentTimeMillis() - startTime;
        DataStatistics state = getState( DataStatistics.class );
        String additionalInformation = Objects.toString( state, "Data statistics is not available." );
        executionMonitor.done( successful, totalTimeMillis, format( "%n%s%nPeak memory usage: %s", additionalInformation,
                bytesToString( peakMemoryUsage ) ) );
        log.info( "Import completed successfully, took " + duration( totalTimeMillis ) + ". " + additionalInformation );
        closeAll( nodeRelationshipCache, nodeLabelsCache, idMapper );
    }
}

