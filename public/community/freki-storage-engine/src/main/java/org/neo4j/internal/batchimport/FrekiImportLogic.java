package org.neo4j.internal.batchimport;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.DefaultHumanUnderstandableExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionSupervisors;
import org.neo4j.internal.freki.FrekiStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.System.currentTimeMillis;

public class FrekiImportLogic extends BaseImportLogic implements Closeable {
    private final Dependencies dependencies = new Dependencies();
    private long startTime;
    private final Map<Class<?>,Object> accessibleState = new HashMap<>();
    public FrekiImportLogic(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, BatchingStoreBase neoStore, Collector badCollector, LogService logService,
                            ExecutionMonitor executionMonitor, Configuration config, Config dbConfig, Monitor monitor,
                            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker) {
        super(fileSystem, databaseLayout, neoStore, badCollector, logService, executionMonitor, config, dbConfig, monitor, pageCacheTracer, memoryTracker);
        if (executionMonitor instanceof DefaultHumanUnderstandableExecutionMonitor)
            executionMonitor = FrekiExecutionMonitors.defaultVisible();
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment( executionMonitor, config );
    }

    @Override
    public void initialize(Input input) throws IOException {
        log.info( "Import starting" );
        startTime = currentTimeMillis();
        Input.Estimates inputEstimates = input.calculateEstimates(new FrekiRecordSizeCalculator());
        super.initialize( input, dependencies);
        dependencies.satisfyDependency( inputEstimates );
        executionMonitor.initialize( dependencies );
    }

    @Override
    public void importNodes() throws IOException {
        Supplier<BaseEntityImporter> importers = () ->
                (new FrekiNodeImporter( basicNeoStore, idMapper, inputIdLookup, storeUpdateMonitor, pageCacheTracer, memoryTracker));
        DataImporter.importNodes( config.maxNumberOfProcessors(),
                input, basicNeoStore, importers, idMapper, badCollector,
                executionMonitor, storeUpdateMonitor, pageCacheTracer, memoryTracker );
        ((FrekiBatchStores)basicNeoStore).flushAndForce( pageCacheTracer.createPageCursorTracer("flush"));
    }

    @Override
    public void importRelationships() throws IOException {
        //basicNeoStore.startFlushingPageCache();
        DataStatistics typeDistribution = new DataStatistics( storeUpdateMonitor, new DataStatistics.RelationshipTypeCount[0] );
        Supplier<BaseEntityImporter> importers = () -> new FrekiRelationshipImporter( basicNeoStore, idMapper, typeDistribution, storeUpdateMonitor,
                pageCacheTracer, memoryTracker, badCollector);
        DataImporter.importRelationships(
             config.maxNumberOfProcessors(),
                input, basicNeoStore, importers, idMapper, badCollector, executionMonitor, storeUpdateMonitor,
                true, pageCacheTracer, memoryTracker  );
        //basicNeoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
        putState( typeDistribution );
    }

    public <T> void putState( T state )
    {
        accessibleState.put( state.getClass(), state );
        dependencies.satisfyDependency( state );
    }

    @Override
    public void calculateNodeDegrees() {

    }

    @Override
    public void buildInternalStructures() {

    }

    @Override
    public void buildCountsStore() {

    }

    @Override
    public void updatePeakMemoryUsage() {

    }

    boolean successful;
    @Override
    public void success() {
        basicNeoStore. success();
        successful = true;
    }

    @Override
    public void close() throws IOException {

    }
}
