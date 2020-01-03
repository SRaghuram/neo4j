/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
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

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.lang.String.format;

public class ClusteredDbmsReconciler extends DbmsReconciler
{
    private final ClusteredMultiDatabaseManager databaseManager;
    private final LogProvider logProvider;
    private final ClusterStateStorageFactory stateStorageFactory;
    private final PanicService panicService;

    ClusteredDbmsReconciler( ClusteredMultiDatabaseManager databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler,
            ClusterStateStorageFactory stateStorageFactory, PanicService panicService )
    {
        super( databaseManager, config, logProvider, scheduler );
        this.databaseManager = databaseManager;
        this.logProvider = logProvider;
        this.stateStorageFactory = stateStorageFactory;
        this.panicService = panicService;
    }

    @Override
    protected EnterpriseDatabaseState getReconcilerEntryFor( NamedDatabaseId namedDatabaseId )
    {
        return currentStates.getOrDefault( namedDatabaseId.name(), initial( namedDatabaseId ) );
    }

    private EnterpriseDatabaseState initial( NamedDatabaseId namedDatabaseId )
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

    @Override
    protected Transitions prepareLifecycleTransitionSteps()
    {
        Transitions standaloneTransitions = super.prepareLifecycleTransitionSteps();
        Transitions clusteredTransitions = Transitions.builder()
                // All transitions from UNKNOWN to $X get deconstructed into UNKNOWN -> DROPPED -> $X
                //     inside Transitions so only actions for this from/to pair need to be specified
                .from( UNKNOWN ).to( DROPPED ).doTransitions( databaseId -> logCleanupAndDrop( databaseId ) )
                // No prepareDrop step needed here as the database will be stopped for store copying anyway
                .from( STORE_COPYING ).to( DROPPED ).doTransitions( databaseId1 -> stop( databaseId1 ), databaseId2 -> drop( databaseId2 ) )
                // Some Cluster components still need stopped when store copying.
                //   This will attempt to stop the kernel database again, but that should be idempotent.
                .from( STORE_COPYING ).to( STOPPED ).doTransitions( databaseId3 -> stop( databaseId3 ) )
                .from( STORE_COPYING ).to( STARTED ).doTransitions( databaseId4 -> startAfterStoreCopy( databaseId4 ) )
                .from( STARTED ).to( STORE_COPYING ).doTransitions( databaseId5 -> stopBeforeStoreCopy( databaseId5 ) )
                .build();

        return standaloneTransitions.extendWith( clusteredTransitions );
    }

    @Override
    protected void panicDatabase( NamedDatabaseId namedDatabaseId, Throwable error )
    {
        var databasePanicker = panicService.panickerFor( namedDatabaseId );
        databasePanicker.panic( error );
    }

    /* Operator Steps */
    private EnterpriseDatabaseState startAfterStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.startDatabaseAfterStoreCopy( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STARTED );
    }

    private EnterpriseDatabaseState stopBeforeStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.stopDatabaseBeforeStoreCopy( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STORE_COPYING );
    }

    private EnterpriseDatabaseState logCleanupAndDrop( NamedDatabaseId namedDatabaseId )
    {
        var log = logProvider.getLog( getClass() );
        log.warn( format( "Pre-existing cluster state found with an unexpected id %s. This may indicate a previous " +
                "DROP operation for %s did not complete. Cleanup of both the database and cluster-sate has been attempted. You may need to re-seed",
                namedDatabaseId.databaseId().uuid(), namedDatabaseId.name() ) );
        databaseManager.dropDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, DROPPED );
    }

    private DatabaseLogProvider databaseLogProvider( NamedDatabaseId namedDatabaseId )
    {
       return new DatabaseLogProvider( new DatabaseNameLogContext( namedDatabaseId ), this.logProvider );
    }
}
