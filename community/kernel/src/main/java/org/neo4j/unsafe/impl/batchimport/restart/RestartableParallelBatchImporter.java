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
import java.util.HashSet;
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
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.NODE;
import static org.neo4j.kernel.impl.store.StoreType.NODE_LABEL;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_ARRAY;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_STRING;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.instantiateNeoStores;

public class RestartableParallelBatchImporter implements BatchImporter
{
    private static final String STATE_NEW_IMPORT = StateStorage.NO_STATE;
    private static final String STATE_START = "start";
    private static final String STATE_FILE_NAME = "state";
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
    }

    @Override
    public void doImport( Input input ) throws IOException
    {
        try ( BatchingNeoStores store = instantiateNeoStores( fileSystem, storeDir, externalPageCache, recordFormats,
                      config, logService, additionalInitialIds, dbConfig );
              ImportLogic logic = new ImportLogic( storeDir, fileSystem, store, config, logService,
                      executionMonitor, recordFormats, input ) )
        {
            StateStorage state = new StateStorage( fileSystem, new File( storeDir, STATE_FILE_NAME ) );
            String stateName = state.get();
            initializeStore( store, stateName );

            switch ( stateName )
            {
            case STATE_NEW_IMPORT:
                setState( store, state, STATE_START );
            case STATE_START:
                logic.importNodes();
                logic.prepareIdMapper();
                logic.importRelationships();
                setState( store, state, STATE_DATA_IMPORT );
                // fall-through to next state

            case STATE_DATA_IMPORT:
                logic.calculateNodeDegrees();
                logic.linkRelationships();
                logic.defragmentRelationshipGroups();
                setState( store, state, STATE_DATA_LINK );
                // fall-through to next state

            case STATE_DATA_LINK:
                logic.buildCountsStore();
                // import completed
                setState( store, state, null /*i.e. remove it*/ );
                break;

            default:
                throw new UnsupportedOperationException( "Unexpected state '" + stateName + "'" );
            }
        }
    }

    private void setState( BatchingNeoStores store, StateStorage state, String stateName ) throws IOException
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

    private void initializeStore( BatchingNeoStores store, String stateName ) throws IOException
    {
        Set<StoreType> storesToKeep = new HashSet<>();

        // Fall through the states, but in reverse order because the longer we've reached in the import the more to keep
        // and it's clearer to specify what to keep instead of what to delete.
        switch ( stateName )
        {
        case STATE_DATA_LINK:
            storesToKeep.addAll( asList( StoreType.RELATIONSHIP_GROUP ) );
            // fall-through to previous state

        case STATE_DATA_IMPORT:
            storesToKeep.addAll( asList(
                    NODE, NODE_LABEL, LABEL_TOKEN, LABEL_TOKEN_NAME,
                    RELATIONSHIP, RELATIONSHIP_TYPE_TOKEN, RELATIONSHIP_TYPE_TOKEN_NAME,
                    PROPERTY, PROPERTY_ARRAY, PROPERTY_STRING, PROPERTY_KEY_TOKEN, PROPERTY_KEY_TOKEN_NAME ) );
            // fall-through to previous state

        case STATE_START:
            storesToKeep.add( StoreType.META_DATA );
            // fall-through to previous state

        case STATE_NEW_IMPORT:
            break;

        default:
            throw new UnsupportedOperationException( "Unknown state '" + stateName + "'" );
        }

        if ( storesToKeep.isEmpty() )
        {
            store.createNew();
        }
        else
        {
            store.pruneAndOpenExistingStore( type -> storesToKeep.contains( type ) );
        }
    }
}
