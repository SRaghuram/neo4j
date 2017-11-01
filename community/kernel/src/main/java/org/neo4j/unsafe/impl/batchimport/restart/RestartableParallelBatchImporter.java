/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.restart;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ImportLogic;
import org.neo4j.unsafe.impl.batchimport.RelationshipTypeDistribution;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static java.util.Arrays.asList;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.PrefetchingIterator.prefetching;
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
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.instantiateNeoStores;

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
    private final File storeDir;
    private final FileSystemAbstraction fileSystem;
    private final Configuration config;
    private final LogService logService;
    private final Config dbConfig;
    private final RecordFormats recordFormats;
    private final ExecutionMonitor executionMonitor;
    private final AdditionalInitialIds additionalInitialIds;
    private final RelationshipTypeDistributionStorage relationshipTypeDistributionStorage;

    public RestartableParallelBatchImporter( File storeDir, FileSystemAbstraction fileSystem, PageCache externalPageCache,
            Configuration config, LogService logService, ExecutionMonitor executionMonitor,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, RecordFormats recordFormats )
    {
        this.externalPageCache = externalPageCache;
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logService = logService;
        this.dbConfig = dbConfig;
        this.recordFormats = recordFormats;
        this.executionMonitor = executionMonitor;
        this.additionalInitialIds = additionalInitialIds;
        this.relationshipTypeDistributionStorage = new RelationshipTypeDistributionStorage( fileSystem,
                new File( storeDir, FILE_NAME_RELATIONSHIP_DISTRIBUTION ) );
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        try ( BatchingNeoStores store = instantiateNeoStores( fileSystem, storeDir, externalPageCache, recordFormats,
                      config, logService, additionalInitialIds, dbConfig );
              ImportLogic logic = new ImportLogic( storeDir, fileSystem, store, config, logService,
                      executionMonitor, recordFormats, input ) )
        {
            StateStorage stateStore = new StateStorage( fileSystem, new File( storeDir, FILE_NAME_STATE ) );

            PrefetchingIterator<State> states = initializeStates( logic );
            Pair<String,byte[]> previousState = stateStore.get();
            fastForwardToLastCompletedState( store, stateStore, previousState.first(), previousState.other(), states );
            runRemainingStates( store, stateStore, previousState.other(), states );

            store.success();
        }
    }

    private PrefetchingIterator<State> initializeStates( ImportLogic logic )
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
                relationshipTypeDistributionStorage.store( logic.getState( RelationshipTypeDistribution.class ) );
            }

            @Override
            void load() throws IOException
            {
                logic.putState( relationshipTypeDistributionStorage.load() );
            }
        } );
        states.add( new State( STATE_DATA_LINK, array(), array( RELATIONSHIP_GROUP ) )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer ) throws IOException
            {
                logic.calculateNodeDegrees();
                int type = fromCheckPoint.length > 0
                        // Looks like we're restarting after one or more rounds of linking
                        ? ByteBuffer.wrap( fromCheckPoint ).getInt()
                        // Looks like we're restarting from some point between data-import done and first round of linking
                        : 0;
                while ( type != -1 )
                {
                    // Make a check point so that we can restart from this relationship type
                    checkPointer.checkPoint( intCheckPoint( type ) );
                    type = logic.linkRelationships( type );
                }
            }
        } );
        states.add( new State( STATE_DEFRAGMENT, array( RELATIONSHIP_GROUP ), array() )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer ) throws IOException
            {
                logic.defragmentRelationshipGroups();
            }
        } );
        states.add( new State( null, array(), array() )
        {
            @Override
            void run( byte[] fromCheckPoint, CheckPointer checkPointer ) throws IOException
            {
                logic.buildCountsStore();
            }
        } );

        return prefetching( states.iterator() );
    }

    private static void fastForwardToLastCompletedState( BatchingNeoStores store, StateStorage stateStore, String stateName, byte[] checkPoint,
            PrefetchingIterator<State> states ) throws IOException
    {
        if ( STATE_NEW_IMPORT.equals( stateName ) )
        {
            stateStore.set( STATE_INIT, EMPTY_BYTE_ARRAY );
            store.createNew();
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
                    store.pruneAndOpenExistingStore(
                            type -> mainStoresToKeep.contains( type ),
                            type -> tempStoresToKeep.contains( type ) );
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

    private static void runRemainingStates( BatchingNeoStores store, StateStorage stateStore, byte[] checkPoint,
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

    private static void writeState( BatchingNeoStores store, StateStorage state, String stateName, byte[] checkPoint ) throws IOException
    {
        store.flushAndForce();
        if ( stateName != null )
        {
            state.set( stateName, checkPoint );
        }
        else
        {
            state.remove();
        }
    }

    private static byte[] intCheckPoint( int type )
    {
        byte[] checkPoint = new byte[Integer.BYTES];
        ByteBuffer.wrap( checkPoint ).putInt( type );
        return checkPoint;
    }
}
