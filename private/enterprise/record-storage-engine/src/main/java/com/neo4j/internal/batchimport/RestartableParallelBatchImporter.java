/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.DataStatistics;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.ImportLogic.Monitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;

import static java.util.Arrays.asList;
import static org.neo4j.internal.batchimport.ImportLogic.instantiateNeoStores;
import static org.neo4j.internal.helpers.ArrayUtil.array;
import static org.neo4j.internal.helpers.collection.Iterators.prefetching;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
import static org.neo4j.kernel.impl.store.StoreType.NODE;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_GROUP;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN_NAME;

public class RestartableParallelBatchImporter implements BatchImporter
{
    static final String FILE_NAME_STATE = "state";
    private static final String FILE_NAME_RELATIONSHIP_DISTRIBUTION = "relationship-type-distribution";

    private static final String STATE_NEW_IMPORT = StateStorage.NO_STATE;
    private static final String STATE_INIT = StateStorage.INIT;
    private static final String STATE_START = "start";
    private static final String STATE_DATA_IMPORT = "data-import";
    private static final String STATE_DATA_LINK = "data-link";
    private static final String STATE_DEFRAGMENT = "defragment";

    private final PageCache externalPageCache;
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fileSystem;
    private final PageCacheTracer pageCacheTracer;
    private final Configuration config;
    private final LogService logService;
    private final Config dbConfig;
    private final RecordFormats recordFormats;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final RelationshipTypeDistributionStorage dataStatisticsStorage;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;
    private final Collector badCollector;
    private final LogFilesInitializer logFilesInitializer;
    private final MemoryTracker memoryTracker;

    public RestartableParallelBatchImporter( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            PageCacheTracer pageCacheTracer, Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats, Monitor monitor, JobScheduler jobScheduler,
            Collector badCollector, LogFilesInitializer logFilesInitializer, MemoryTracker memoryTracker )
    {
        this.externalPageCache = externalPageCache;
        this.databaseLayout = databaseLayout;
        this.fileSystem = fileSystem;
        this.pageCacheTracer = pageCacheTracer;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.recordFormats = recordFormats;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.monitor = monitor;
        this.dataStatisticsStorage = new RelationshipTypeDistributionStorage( fileSystem,
                this.databaseLayout.databaseDirectory().resolve( FILE_NAME_RELATIONSHIP_DISTRIBUTION ), memoryTracker );
        this.jobScheduler = jobScheduler;
        this.badCollector = badCollector;
        this.logFilesInitializer = logFilesInitializer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        try ( BatchingNeoStores store = instantiateNeoStores( fileSystem, databaseLayout, externalPageCache, pageCacheTracer, recordFormats,
                      config, logService, additionalInitialIds, dbConfig, jobScheduler, memoryTracker );
              ImportLogic logic = new ImportLogic( databaseLayout, store, config, dbConfig, logService,
                      executionMonitor, recordFormats, badCollector, monitor, pageCacheTracer, memoryTracker ) )
        {
            StateStorage stateStore = new StateStorage( fileSystem, databaseLayout.databaseDirectory().resolve( FILE_NAME_STATE ), memoryTracker );

            PrefetchingIterator<State> states = initializeStates( logic, store );
            Pair<String,byte[]> previousState = stateStore.get();
            fastForwardToLastCompletedState( store, stateStore, previousState.first(), previousState.other(), states );
            logic.initialize( input );
            runRemainingStates( store, stateStore, previousState.other(), states );

            logic.success();
        }
    }

    private PrefetchingIterator<State> initializeStates( ImportLogic logic, BatchingNeoStores store )
    {
        List<State> states = new ArrayList<>();

        states.add( new State( STATE_INIT, array(), array() ) );
        states.add( new State( STATE_START, array( META_DATA ), array() ) );
        states.add( new State( STATE_DATA_IMPORT, array(
                NODE, NODE_LABEL, LABEL_TOKEN, LABEL_TOKEN_NAME,
                RELATIONSHIP, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_NAME,
                PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_NAME ), array() )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer ) throws IOException
            {
                logic.importNodes();
                logic.prepareIdMapper();
                logic.importRelationships();
            }

            @Override
            void save() throws IOException
            {
                dataStatisticsStorage.store( logic.getState( DataStatistics.class ) );
            }

            @Override
            void load() throws IOException
            {
                logic.putState( dataStatisticsStorage.load() );
            }
        } );
        states.add( new State( STATE_DATA_LINK, array(), array( RELATIONSHIP_GROUP ) )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer )
            {
                logic.calculateNodeDegrees();
                int type = 0;
                while ( type != -1 )
                {
                    type = logic.linkRelationships( type );
                }
            }
        } );
        states.add( new State( STATE_DEFRAGMENT, array( RELATIONSHIP_GROUP ), array() )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer )
            {
                logic.defragmentRelationshipGroups();
            }
        } );
        states.add( new State( null, array(), array() )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer )
            {
                logic.buildCountsStore();
                logFilesInitializer.initializeLogFiles( databaseLayout, store.getNeoStores().getMetaDataStore(), fileSystem );
            }
        } );

        return prefetching( states.iterator() );
    }

    private static void fastForwardToLastCompletedState( BatchingNeoStores store, StateStorage stateStore, String stateName,
            byte[] checkPoint, PrefetchingIterator<State> states ) throws IOException
    {
        if ( STATE_NEW_IMPORT.equals( stateName ) )
        {
            store.createNew();
            stateStore.set( STATE_INIT, EMPTY_BYTE_ARRAY );
        }
        else
        {
            Set<StoreType> mainStoresToKeep = new HashSet<>();
            Set<StoreType> tempStoresToKeep = new HashSet<>();
            while ( states.hasNext() )
            {
                State state = states.peek();
                // Regardless of existence of check point we'll have to keep these store types because this state,
                // which messes with these stores is either completed or mid-way through it.
                mainStoresToKeep.addAll( asList( state.completesMainStoreTypes() ) );
                tempStoresToKeep.addAll( asList( state.completesTempStoreTypes() ) );
                state.load();
                if ( state.name().equals( stateName ) )
                {
                    // Prepare to start from this state
                    store.pruneAndOpenExistingStore( mainStoresToKeep::contains, tempStoresToKeep::contains );
                    if ( checkPoint.length == 0 )
                    {
                        // Advance the states iterator since there's no check point, i.e. this state is completed.
                        // We'll leave the iterator starting from the next state.
                        states.next();
                    }
                    // else leave the states iterator on this state so that it can be run from its last checkpoint.
                    break;
                }
                // We're way past this state, advance to the next
                states.next();
            }
        }
    }

    private void runRemainingStates( BatchingNeoStores store, StateStorage stateStore, byte[] checkPoint,
            Iterator<State> states ) throws IOException
    {
        while ( states.hasNext() )
        {
            State state = states.next();
            String stateName = state.name();
            state.run( checkPoint, cp -> writeState( store, stateStore, stateName, cp ) );
            state.save();
            writeState( store, stateStore, stateName, checkPoint = EMPTY_BYTE_ARRAY );
        }
    }

    private void writeState( BatchingNeoStores store, StateStorage state, String stateName, byte[] checkPoint ) throws IOException
    {
        store.flushAndForce( NULL );
        if ( stateName != null )
        {
            state.set( stateName, checkPoint );
        }
        else
        {
            state.remove();
            dataStatisticsStorage.remove();
        }
    }
}
