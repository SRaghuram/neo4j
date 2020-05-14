/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;

public class ClusteredDbmsReconciler extends DbmsReconciler
{
    private final LogProvider logProvider;
    private final ClusterStateStorageFactory stateStorageFactory;

    ClusteredDbmsReconciler( ClusteredMultiDatabaseManager databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler,
                            ClusterStateStorageFactory stateStorageFactory, TransitionsTable transitionsTable )
    {
        super( databaseManager, config, logProvider, scheduler, transitionsTable );
        this.logProvider = logProvider;
        this.stateStorageFactory = stateStorageFactory;
    }

    @Override
    protected EnterpriseDatabaseState initialReconcilerEntry( NamedDatabaseId namedDatabaseId )
    {
        var raftIdOpt = readRaftIdForDatabase( namedDatabaseId, databaseLogProvider( namedDatabaseId ) );
        if ( raftIdOpt.isPresent() )
        {
            var raftId = raftIdOpt.get();
            var previousDatabaseId = DatabaseIdFactory.from( namedDatabaseId.name(), raftId.uuid() );
            if ( !Objects.equals( namedDatabaseId, previousDatabaseId ) )
            {
                return EnterpriseDatabaseState.unknown( previousDatabaseId );
            }
        }
        return EnterpriseDatabaseState.initial( namedDatabaseId );
    }

    private Optional<RaftId> readRaftIdForDatabase( NamedDatabaseId namedDatabaseId, DatabaseLogProvider logProvider )
    {
        var databaseName = namedDatabaseId.name();
        var raftIdStorage = stateStorageFactory.createRaftIdStorage( databaseName, logProvider );

        if ( !raftIdStorage.exists() )
        {
            return Optional.empty();
        }

        try
        {
            return Optional.ofNullable( raftIdStorage.readState() );
        }
        catch ( IOException e )
        {
            throw new DatabaseManagementException( format( "Unable to read potentially dirty cluster state while starting %s.", databaseName ) );
        }
    }

    private DatabaseLogProvider databaseLogProvider( NamedDatabaseId namedDatabaseId )
    {
       return new DatabaseLogProvider( new DatabaseNameLogContext( namedDatabaseId ), this.logProvider );
    }
}
