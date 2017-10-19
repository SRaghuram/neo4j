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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
    private static final String FILE_NAME_STATE = "state";
    private static final String FILE_NAME_RELATIONSHIP_DISTRIBUTION = "relationship-type-distribution";

    private static final String STATE_NEW_IMPORT = StateStorage.NO_STATE;
    private static final String STATE_START = "start";
    private static final String STATE_DATA_IMPORT = "data-import";
    private static final String STATE_DATA_LINK = "data-link";

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

            Iterator<State> states = initializeStates( logic );
            fastForwardToLastCompletedState( store, stateStore.get(), states );
            runRemainingStates( store, stateStore, states );
        }
    }

    private Iterator<State> initializeStates( ImportLogic logic )
    {
        List<State> states = new ArrayList<>();

        states.add( new State( STATE_START, META_DATA ) );
        states.add( new State( STATE_DATA_IMPORT,
                NODE, NODE_LABEL, LABEL_TOKEN, LABEL_TOKEN_NAME,
                RELATIONSHIP, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_NAME,
                PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_NAME )
        {
            @Override
            void run() throws IOException
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
        states.add( new State( STATE_DATA_LINK, RELATIONSHIP_GROUP )
        {
            @Override
            void run() throws IOException
            {
                logic.calculateNodeDegrees();
                logic.linkRelationships();
                logic.defragmentRelationshipGroups();
            }
        } );
        states.add( new State( null )
        {
            @Override
            void run() throws IOException
            {
                logic.buildCountsStore();
            }
        } );

        return states.iterator();
    }

    private static void fastForwardToLastCompletedState( BatchingNeoStores store, String stateName, Iterator<State> states )
            throws IOException
    {
        if ( STATE_NEW_IMPORT.equals( stateName ) )
        {
            store.createNew();
        }
        else
        {
            Set<StoreType> storesToKeep = new HashSet<>();
            while ( states.hasNext() )
            {
                State state = states.next();
                storesToKeep.addAll( asList( state.completesStoreTypes() ) );
                state.load();
                if ( state.name().equals( stateName ) )
                {
                    // Prepare to start from this state
                    store.pruneAndOpenExistingStore( type -> storesToKeep.contains( type ) );
                    break;
                }
            }
        }
    }

    private static void runRemainingStates( BatchingNeoStores store, StateStorage stateStore, Iterator<State> states ) throws IOException
    {
        while ( states.hasNext() )
        {
            State state = states.next();
            state.run();
            state.save();
            markStateCompleted( store, stateStore, state.name() );
        }
    }

    private static void markStateCompleted( BatchingNeoStores store, StateStorage state, String stateName ) throws IOException
    {
        store.flushAndForce();
        if ( stateName != null )
        {
            state.set( stateName );
        }
        else
        {
            state.remove();
        }
    }
}
