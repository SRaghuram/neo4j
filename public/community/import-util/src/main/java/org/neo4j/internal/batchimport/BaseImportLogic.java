package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
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

import static org.neo4j.internal.batchimport.cache.NumberArrayFactory.auto;

public abstract class BaseImportLogic implements ImportLogicInterface {
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
    protected IdMapper idMapper;
    protected BatchingStoreBase basicNeoStore;
    protected Collector badCollector;
    protected Configuration config;
    protected NumberArrayFactory numberArrayFactory;
    protected DataImporter.Monitor storeUpdateMonitor = new DataImporter.Monitor();
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

    public void initialize( Input input, Dependencies dependencies )
    {
        log.info( "Import starting" );
        //startTime = currentTimeMillis();
        this.input = input;
        PageCacheArrayFactoryMonitor numberArrayFactoryMonitor = new PageCacheArrayFactoryMonitor();
        numberArrayFactory = auto( basicNeoStore.getPageCache(), pageCacheTracer, databaseLayout.databaseDirectory(), config.allowCacheAllocationOnHeap(),
                numberArrayFactoryMonitor );
        inputIdLookup = getNodeInputIdLookup();
        // Some temporary caches and indexes in the import
        idMapper = instantiateIdMapper( input );
        nodeRelationshipCache = new NodeRelationshipCache( numberArrayFactory, dbConfig.get( GraphDatabaseSettings.dense_node_threshold ), memoryTracker);
        dependencies.satisfyDependencies( idMapper, basicNeoStore, nodeRelationshipCache, numberArrayFactoryMonitor );
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
                executeStage( new DeleteDuplicateNodesStage( config, duplicateNodeIds, basicNeoStore, storeUpdateMonitor, pageCacheTracer ) );
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
}
