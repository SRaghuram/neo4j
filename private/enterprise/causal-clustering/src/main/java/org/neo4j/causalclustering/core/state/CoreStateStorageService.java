/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.collection.Iterators.asSet;

public class CoreStateStorageService
{
    private static final Set<CoreStateFiles> simpleStorage = asSet( CoreStateFiles.CLUSTER_ID, CoreStateFiles.DB_NAME, CoreStateFiles.CORE_MEMBER_ID );
    private static final Set<CoreStateFiles> perDbStorage = asSet( CoreStateFiles.ID_ALLOCATION, CoreStateFiles.LOCK_TOKEN );
    private final Map<CoreStateFiles,Map<String,StateStorage>> cachedStorage;
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ClusterStateDirectory clusterStateDirectory;
    private final LifeSupport life;
    private final Config config;

    public CoreStateStorageService( FileSystemAbstraction fs, ClusterStateDirectory clusterStateDirectory, LifeSupport lifeSupport,
            LogProvider logProvider, Config config )
    {
        this.cachedStorage = new HashMap<>();
        this.fs = fs;
        this.logProvider = logProvider;
        this.clusterStateDirectory = clusterStateDirectory;
        this.config = config;
        this.life = lifeSupport;
    }

    public <E> SimpleStorage<E> simpleStorage( CoreStateFiles<E> type )
    {
        if ( type.fileType() == CoreStateFiles.FileType.SIMPLE )
        {
            return (SimpleStorage<E>) stateStorage( type, null );
        }

        throw new UnsupportedOperationException( String.format( "You cannot instantiate SimpleStorage for core state of type %s", type ) );
    }

    @SuppressWarnings( "SameParameterValue" )
    <E> DurableStateStorage<E> durableStorage( CoreStateFiles<E> type )
    {
        return durableStorage( type, null );
    }

    <E> DurableStateStorage<E> durableStorage( CoreStateFiles<E> type, String databaseName )
    {
        if ( type.fileType() == CoreStateFiles.FileType.SIMPLE )
        {
            throw new UnsupportedOperationException( String.format( "You cannot instantiate DurableStorage for core state of type %s", type ) );
        }

        return (DurableStateStorage<E>) stateStorage( type, databaseName );
    }

    public <E> StateStorage<E> stateStorage( CoreStateFiles<E> type )
    {
        return stateStorage( type, null );
    }

    <E> StateStorage<E> stateStorage( CoreStateFiles<E> type, String databaseName )
    {
        Map<String,StateStorage> perDbStorage = cachedStorage.computeIfAbsent( type, ignored -> new HashMap<>() );

        //noinspection unchecked Casting here is safe because we guarantee the types are equivalent at the only insertion point to the map.
        StateStorage<E> store = (StateStorage<E>) perDbStorage.get( databaseName );
        if ( store == null )
        {
            store = createNewStorage( type, databaseName );
            perDbStorage.put( databaseName, store );
        }
        return store;
    }

    public void migrateIfNecessary( String databaseName )
    {
        migrateDatabaseStateIfNeeded( databaseName );
    }

    private <E> StateStorage<E> createNewStorage( CoreStateFiles<E> type, String databaseName )
    {
        DurableStateStorage<E> durableStore;
        if ( simpleStorage.contains( type ) )
        {
            File simpleStateDirectory = type.at( clusterStateDirectory.get() );
            File simpleStateFile = new File( simpleStateDirectory, type.baseName() );
            return new SimpleFileStorage<>( fs, simpleStateFile, type.marshal(), logProvider );
        }
        else if ( perDbStorage.contains( type ) )
        {
            File databaseStateDirectory = clusterStateDirectory.databaseStateDirectory( databaseName );
            durableStore = new DurableStateStorage<>( fs, databaseStateDirectory, type, type.rotationSize( config ), logProvider );
        }
        else
        {
            durableStore = new DurableStateStorage<>( fs, clusterStateDirectory.get(), type, type.rotationSize( config ), logProvider );
        }
        life.add( durableStore );
        return durableStore;
    }

    /**
     * <pre>
     * Databases-specific state previously lived directly at
     *      cluster-state/
     *
     * but has now moved down to
     *      cluster-state/db/<database-name>     (typically /graph.db)
     */
    private void migrateDatabaseStateIfNeeded( String databaseName )
    {
        for ( CoreStateFiles state : perDbStorage )
        {
            File oldDir = new File( clusterStateDirectory.get(), state.directoryName() );

            if ( !fs.fileExists( oldDir ) )
            {
                continue;
            }

            File dbStateRootDir = clusterStateDirectory.databaseStateDirectory( databaseName );

            try
            {
                fs.moveToDirectory( oldDir, dbStateRootDir );
            }
            catch ( IOException e )
            {
                throw new ClusterStateException( e );
            }
        }
    }
}
