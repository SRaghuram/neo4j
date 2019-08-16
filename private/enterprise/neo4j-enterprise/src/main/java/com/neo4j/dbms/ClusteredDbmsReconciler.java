/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.causalclustering.identity.RaftId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseNameLogContext;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static com.neo4j.dbms.OperatorState.STORE_COPYING;
import static com.neo4j.dbms.OperatorState.UNKNOWN;
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
    protected DatabaseState getReconcilerEntryFor( DatabaseId databaseId )
    {
        return currentStates.getOrDefault( databaseId.name(), initial( databaseId ) );
    }

    private DatabaseState initial( DatabaseId databaseId )
    {
        var raftIdOpt = readRaftIdForDatabase( databaseId, databaseLogProvider( databaseId ) );
        if ( raftIdOpt.isPresent() )
        {
            var raftId = raftIdOpt.get();
            var previousDatabaseId = DatabaseIdFactory.from( databaseId.name(), raftId.uuid() );
            if ( !Objects.equals( databaseId, previousDatabaseId ) )
            {
                return DatabaseState.unknown( previousDatabaseId );
            }
        }
        return DatabaseState.initial( databaseId );
    }

    private Optional<RaftId> readRaftIdForDatabase( DatabaseId databaseId, DatabaseLogProvider logProvider )
    {
        var databaseName = databaseId.name();
        var raftIdStorage = stateStorageFactory.createRaftIdStorage( databaseName, logProvider );

        if ( ! raftIdStorage.exists() )
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
                .from( UNKNOWN ).to( DROPPED ).doTransitions( this::logCleanupAndDrop )
                // No prepareDrop step needed here as the database will be stopped for store copying anyway
                .from( STORE_COPYING ).to( DROPPED ).doTransitions( this::stop, this::drop )
                // Some Cluster components still need stopped when store copying.
                //   This will attempt to stop the kernel database again, but that should be idempotent.
                .from( STORE_COPYING ).to( STOPPED ).doTransitions( this::stop )
                .from( STORE_COPYING ).to( STARTED ).doTransitions( this::startAfterStoreCopy )
                .from( STARTED ).to( STORE_COPYING ).doTransitions( this::stopBeforeStoreCopy )
                .build();

        return standaloneTransitions.extendWith( clusteredTransitions );
    }

    @Override
    protected void panicDatabase( DatabaseId databaseId, Throwable error )
    {
        var databasePanicker = panicService.panickerFor( databaseId );
        databasePanicker.panic( error );
    }

    /* Operator Steps */
    private DatabaseState startAfterStoreCopy( DatabaseId databaseId )
    {
        databaseManager.startDatabaseAfterStoreCopy( databaseId );
        return new DatabaseState( databaseId, STARTED );
    }

    private DatabaseState stopBeforeStoreCopy( DatabaseId databaseId )
    {
        databaseManager.stopDatabaseBeforeStoreCopy( databaseId );
        return new DatabaseState( databaseId, STORE_COPYING );
    }

    private DatabaseState logCleanupAndDrop( DatabaseId databaseId )
    {
        var log = logProvider.getLog( getClass() );
        log.warn( format( "Pre-existing cluster state found with an unexpected id %s. This may indicate a previous " +
                "DROP operation for %s did not complete. Cleanup of both the database and cluster-sate has been attempted. You may need to re-seed",
                databaseId.uuid(), databaseId.name() ) );
        databaseManager.dropDatabase( databaseId );
        return new DatabaseState( databaseId, DROPPED );
    }

    private DatabaseLogProvider databaseLogProvider( DatabaseId databaseId )
    {
       return new DatabaseLogProvider( new DatabaseNameLogContext( databaseId ), this.logProvider );
    }

}
