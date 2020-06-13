/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.IntSet;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.cache.GatheringMemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NodeType;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.cache.PageCacheArrayFactoryMonitor;
import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.cache.idmapping.IdMappers;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.EstimationSanityChecker;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.*;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;

import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactory.auto;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.io.ByteUnit.bytesToString;
import static org.neo4j.io.IOUtils.closeAll;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;

/**
 * Contains all algorithms and logic for doing an import. It exposes all stages as methods so that
 * it's possible to implement a {@link BatchImporter} which calls those.
 * This class has state which typically gets modified in each invocation of an import method.
 *
 * To begin with the methods are fairly coarse-grained, but can and probably will be split up into smaller parts
 * to allow external implementors have greater control over the flow.
 */
public class ImportLogic extends BaseImportLogic implements Closeable
{
    private final BatchingNeoStores neoStore;
    private final RecordFormats recordFormats;
    private final long maxMemory;

    // components which may get assigned and unassigned in some methods
    private long availableMemoryForLinking;

    /**
     * @param fileSystem file system abstraction
     * @param databaseLayout directory which the db will be created in.
     * @param neoStore {@link BatchingNeoStores} to import into.
     * @param config import-specific {@link Configuration}.
     * @param dbConfig database config
     * @param logService {@link LogService} to use.
     * @param executionMonitor {@link ExecutionMonitor} to follow progress as the import proceeds.
     * @param badCollector {@link Collector} for bad entries.
     * @param monitor {@link Monitor} for some events.
     * @param pageCacheTracer
     * @param memoryTracker
     */
    public ImportLogic(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, BatchingStoreBase neoStore,
                       Configuration config, Config dbConfig, LogService logService,
                       ExecutionMonitor executionMonitor, Collector badCollector, BaseImportLogic.Monitor monitor, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        super(fileSystem, databaseLayout, neoStore, badCollector, logService, executionMonitor, config, dbConfig, monitor, pageCacheTracer, memoryTracker );
        this.neoStore = (BatchingNeoStores) neoStore;

        this.maxMemory = config.maxMemoryUsage();
        this.recordFormats = RecordFormatSelector.selectForConfig(fileSystem, dbConfig);
        if (executionMonitor instanceof DefaultHumanUnderstandableExecutionMonitor)
            executionMonitor = ExecutionMonitors.defaultVisible();
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment( executionMonitor, config );
    }

    public void initialize( Input input ) throws IOException
    {
        log.info( "Import starting" );
        startTime = currentTimeMillis();
        super.initialize( input );
        Input.Estimates inputEstimates = input.calculateEstimates( neoStore.getPropertyStore().newValueEncodedSizeCalculator() );

        // Sanity checking against estimates
        new EstimationSanityChecker( recordFormats, monitor ).sanityCheck( inputEstimates );
        new HeapSizeSanityChecker( monitor ).sanityCheck( inputEstimates, new RecordSizes(recordFormats.node().getRecordSize(NO_STORE_HEADER),
                        recordFormats.relationship().getRecordSize(NO_STORE_HEADER),
                        recordFormats.property().getRecordSize(NO_STORE_HEADER)), neoStore,
                nodeRelationshipCache.memoryEstimation( inputEstimates.numberOfNodes() ),
                idMapper.memoryEstimation( inputEstimates.numberOfNodes() ) );

        dependencies.satisfyDependencies( inputEstimates );

        if ( neoStore.determineDoubleRelationshipRecordUnits( inputEstimates ) )
        {
            monitor.doubleRelationshipRecordUnitsEnabled();
        }

        executionMonitor.initialize( dependencies );
    }

    /**
     * Imports nodes w/ their properties and labels from {@link Input#nodes(Collector)}. This will as a side-effect populate the {@link IdMapper},
     * to later be used for looking up ID --> nodeId in {@link #importRelationships()}. After a completed node import,
     * {@link #prepareIdMapper()} must be called.
     *
     * @throws IOException on I/O error.
     */
    public void importNodes() throws IOException
    {
        // Import nodes, properties, labels
        neoStore.startFlushingPageCache();
        Supplier<BaseEntityImporter> importers = () -> new NodeImporter( neoStore, idMapper, inputIdLookup, storeUpdateMonitor, pageCacheTracer, memoryTracker  );
        DataImporter.importNodes( config.maxNumberOfProcessors(), input, neoStore, importers, idMapper, badCollector, executionMonitor,
                pageCacheTracer, memoryTracker  );
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
    }


    /**
     * Uses {@link IdMapper} as lookup for ID --> nodeId and imports all relationships from {@link Input#relationships(Collector)}
     * and writes them into the {@link RelationshipStore}. No linking between relationships is done in this method,
     * it's done later in {@link #linkRelationships(int)}.
     *
     * @throws IOException on I/O error.
     */
    public void importRelationships() throws IOException
    {
        // Import relationships (unlinked), properties
        neoStore.startFlushingPageCache();
        DataStatistics typeDistribution = new DataStatistics( storeUpdateMonitor, new DataStatistics.RelationshipTypeCount[0] );
        Supplier<BaseEntityImporter> importers = () -> new RelationshipImporter( neoStore, idMapper, typeDistribution, storeUpdateMonitor,
                badCollector, !badCollector.isCollectingBadRelationships(), neoStore.usesDoubleRelationshipRecordUnits(), pageCacheTracer, memoryTracker  );
        DataImporter.importRelationships(
                config.maxNumberOfProcessors(), input, neoStore, importers, idMapper, badCollector, executionMonitor,
                !badCollector.isCollectingBadRelationships(), pageCacheTracer, memoryTracker  );
        neoStore.stopFlushingPageCache();
        updatePeakMemoryUsage();
        idMapper.close();
        idMapper = null;
        putState( typeDistribution );
    }

    /**
     * Populates {@link NodeRelationshipCache} with node degrees, which is required to know how to physically layout each
     * relationship chain. This is required before running {@link #linkRelationships(int)}.
     */
    public void calculateNodeDegrees()
    {
        Configuration relationshipConfig =
                configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        nodeRelationshipCache.setNodeCount( neoStore.getNodeStore().getHighId() );
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeRelationshipCache );
        NodeDegreeCountStage nodeDegreeStage = new NodeDegreeCountStage( relationshipConfig,
                neoStore.getRelationshipStore(), nodeRelationshipCache, memoryUsageStats, pageCacheTracer );
        executeStage( nodeDegreeStage );
        nodeRelationshipCache.countingCompleted();
        availableMemoryForLinking = maxMemory - totalMemoryUsageOf( nodeRelationshipCache, neoStore );
    }

    /**
     * Performs one round of linking together relationships with each other. Number of rounds required
     * is dictated by available memory. The more dense nodes and relationship types, the more memory required.
     * Every round all relationships of one or more types are linked.
     *
     * Links together:
     * <ul>
     * <li>
     * Relationship <--> Relationship. Two sequential passes are made over the relationship store.
     * The forward pass links next pointers, each next pointer pointing "backwards" to lower id.
     * The backward pass links prev pointers, each prev pointer pointing "forwards" to higher id.
     * </li>
     * Sparse Node --> Relationship. Sparse nodes are updated with relationship heads of completed chains.
     * This is done in the first round only, if there are multiple rounds.
     * </li>
     * </ul>
     *
     * A linking loop (from external caller POV) typically looks like:
     * <pre>
     * int type = 0;
     * do
     * {
     *    type = logic.linkRelationships( type );
     * }
     * while ( type != -1 );
     * </pre>
     *
     * @param startingFromType relationship type to start from.
     * @return the next relationship type to start linking and, if != -1, should be passed into next call to this method.
     */
    public int linkRelationships( int startingFromType )
    {
        assert startingFromType >= 0 : startingFromType;

        // Link relationships together with each other, their nodes and their relationship groups
        DataStatistics relationshipTypeDistribution = getState( DataStatistics.class );
        MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeRelationshipCache );

        // Figure out which types we can fit in node-->relationship cache memory.
        // Types go from biggest to smallest group and so towards the end there will be
        // smaller and more groups per round in this loop
        int upToType = nextSetOfTypesThatFitInMemory(
                relationshipTypeDistribution, startingFromType, availableMemoryForLinking, nodeRelationshipCache.getNumberOfDenseNodes() );

        final IntSet typesToLinkThisRound = relationshipTypeDistribution.types( startingFromType, upToType );
        int typesImported = typesToLinkThisRound.size();
        boolean thisIsTheFirstRound = startingFromType == 0;
        boolean thisIsTheOnlyRound = thisIsTheFirstRound && upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes();

        Configuration relationshipConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipStore() );
        Configuration nodeConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getNodeStore() );
        Configuration groupConfig = configWithRecordsPerPageBasedBatchSize( config, neoStore.getRelationshipGroupStore() );

        nodeRelationshipCache.setForwardScan( true, true/*dense*/ );
        String range = typesToLinkThisRound.size() == 1
                ? String.valueOf( oneBased( startingFromType ) )
                : oneBased( startingFromType ) + "-" + (startingFromType + typesImported);
        String topic = " " + range + "/" + relationshipTypeDistribution.getNumberOfRelationshipTypes();
        int nodeTypes = thisIsTheFirstRound ? NodeType.NODE_TYPE_ALL : NodeType.NODE_TYPE_DENSE;
        Predicate<RelationshipRecord> readFilter = thisIsTheFirstRound
                ? alwaysTrue() // optimization when all rels are imported in this round
                : record -> typesToLinkThisRound.contains( record.getType() );
        Predicate<RelationshipRecord> denseChangeFilter = thisIsTheOnlyRound
                ? alwaysTrue() // optimization when all rels are imported in this round
                : record -> typesToLinkThisRound.contains( record.getType() );

        // LINK Forward
        RelationshipLinkforwardStage linkForwardStage = new RelationshipLinkforwardStage( topic, relationshipConfig,
                neoStore, nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes, pageCacheTracer,
                new RelationshipLinkingProgress(), memoryUsageStats );
        executeStage( linkForwardStage );

        // Write relationship groups cached from the relationship import above
        executeStage( new RelationshipGroupStage( topic, groupConfig,
                neoStore.getTemporaryRelationshipGroupStore(), nodeRelationshipCache, pageCacheTracer ) );
        if ( thisIsTheFirstRound )
        {
            // Set node nextRel fields for sparse nodes
            executeStage( new SparseNodeFirstRelationshipStage( nodeConfig, neoStore.getNodeStore(),
                    nodeRelationshipCache, pageCacheTracer ) );
        }

        // LINK backward
        nodeRelationshipCache.setForwardScan( false, true/*dense*/ );
        executeStage( new RelationshipLinkbackStage( topic, relationshipConfig, neoStore,
                nodeRelationshipCache, readFilter, denseChangeFilter, nodeTypes, pageCacheTracer,
                new RelationshipLinkingProgress(), memoryUsageStats ) );

        updatePeakMemoryUsage();

        if ( upToType == relationshipTypeDistribution.getNumberOfRelationshipTypes() )
        {
            // This means that we've linked all the types
            nodeRelationshipCache.close();
            nodeRelationshipCache = null;
            return -1;
        }

        return upToType;
    }

    /**
     * Links relationships of all types, potentially doing multiple passes, each pass calling {@link #linkRelationships(int)}
     * with a type range.
     */
    public void linkRelationshipsOfAllTypes()
    {
        int type = 0;
        do
        {
            type = linkRelationships( type );
        }
        while ( type != -1 );
    }

    /**
     * Convenience method (for code reading) to have a zero-based value become one based (for printing/logging).
     */
    private static int oneBased( int value )
    {
        return value + 1;
    }

    /**
     * @return index (into {@link DataStatistics}) of last relationship type that fit in memory this round.
     */
    static int nextSetOfTypesThatFitInMemory( DataStatistics typeDistribution, int startingFromType,
            long freeMemoryForDenseNodeCache, long numberOfDenseNodes )
    {
        assert startingFromType >= 0 : startingFromType;

        long currentSetOfRelationshipsMemoryUsage = 0;
        int numberOfTypes = typeDistribution.getNumberOfRelationshipTypes();
        int toType = startingFromType;
        for ( ; toType < numberOfTypes; toType++ )
        {
            // Calculate worst-case scenario
            DataStatistics.RelationshipTypeCount type = typeDistribution.get( toType );
            long relationshipCountForThisType = type.getCount();
            long memoryUsageForThisType = NodeRelationshipCache.calculateMaxMemoryUsage( numberOfDenseNodes, relationshipCountForThisType );
            long memoryUsageUpToAndIncludingThisType =
                    currentSetOfRelationshipsMemoryUsage + memoryUsageForThisType;
            if ( memoryUsageUpToAndIncludingThisType > freeMemoryForDenseNodeCache &&
                    currentSetOfRelationshipsMemoryUsage > 0 )
            {
                // OK the current set of types is enough to fill the cache
                break;
            }

            currentSetOfRelationshipsMemoryUsage += memoryUsageForThisType;
        }
        return toType;
    }

    /**
     * Optimizes the relationship groups store by physically locating groups for each node together.
     */
    public void defragmentRelationshipGroups()
    {
        // Defragment relationships groups for better performance
        new RelationshipGroupDefragmenter( config, executionMonitor, RelationshipGroupDefragmenter.Monitor.EMPTY, numberArrayFactory, pageCacheTracer,
                memoryTracker ).run( max( maxMemory, peakMemoryUsage ), neoStore, neoStore.getNodeStore().getHighId() );
    }

    /**
     * Builds the counts store. Requires that {@link #importNodes()} and {@link #importRelationships()} has run.
     */
    public void buildCountsStore()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( IMPORT_COUNT_STORE_REBUILD_TAG ) )
        {
            neoStore.buildCountsStore( new CountsBuilder()
            {
                @Override
                public void initialize( CountsAccessor.Updater updater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
                {
                    MigrationProgressMonitor progressMonitor = MigrationProgressMonitor.SILENT;
                    nodeLabelsCache = new NodeLabelsCache( numberArrayFactory, neoStore.getLabelRepository().getHighId(), memoryTracker );
                    MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( neoStore, nodeLabelsCache );
                    executeStage( new NodeCountsAndLabelIndexBuildStage( config, nodeLabelsCache, neoStore.getNodeStore(),
                            neoStore.getLabelRepository().getHighId(),
                            updater, progressMonitor.startSection( "Nodes" ), neoStore.getLabelScanStore(), pageCacheTracer, memoryUsageStats ) );
                    // Count label-[type]->label
                    executeStage( new RelationshipCountsAndTypeIndexBuildStage( config, nodeLabelsCache, neoStore.getRelationshipStore(),
                            neoStore.getLabelRepository().getHighId(),
                            neoStore.getRelationshipTypeRepository().getHighId(), updater, numberArrayFactory,
                            progressMonitor.startSection( "Relationships" ), neoStore.getRelationshipTypeScanStore(), pageCacheTracer, memoryTracker ) );
                }

                @Override
                public long lastCommittedTxId()
                {
                    return neoStore.getLastCommittedTransactionId();
                }
            }, pageCacheTracer, cursorTracer, memoryTracker );
        }
    }

    public void success()
    {
        neoStore.success();
        successful = true;
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

    public void updatePeakMemoryUsage()
    {
        peakMemoryUsage = max( peakMemoryUsage, totalMemoryUsageOf( nodeRelationshipCache, idMapper, neoStore ) );
    }

    public static BatchingNeoStores instantiateNeoStores( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
                                                          PageCache externalPageCache, PageCacheTracer cacheTracer, Configuration config,
                                                          LogService logService, AdditionalInitialIds additionalInitialIds, Config dbConfig, JobScheduler scheduler, MemoryTracker memoryTracker )
    {
        RecordFormats recordFormats = RecordFormatSelector.selectForConfig(fileSystem, dbConfig);
        if ( externalPageCache == null )
        {
            return BatchingNeoStores.batchingNeoStores( fileSystem, databaseLayout, recordFormats, config, logService,
                    additionalInitialIds, dbConfig, scheduler, cacheTracer, memoryTracker );
        }

        return BatchingNeoStores.batchingNeoStoresWithExternalPageCache( fileSystem, externalPageCache,
                cacheTracer, databaseLayout, recordFormats, config, logService, additionalInitialIds, dbConfig, memoryTracker );
    }

    private static long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... users )
    {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            if ( user != null )
            {
                user.acceptMemoryStatsVisitor( total );
            }
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
    }

    private static Configuration configWithRecordsPerPageBasedBatchSize( Configuration source, RecordStore<?> store )
    {
        return Configuration.withBatchSize( source, store.getRecordsPerPage() * 10 );
    }

    @Override
    public void buildInternalStructures()
    {
        linkRelationshipsOfAllTypes();
        defragmentRelationshipGroups();
    }
}
