/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;

import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static com.neo4j.dbms.OperatorState.STORE_COPYING;

public class ClusteredDbmsReconciler extends DbmsReconciler
{
    private final ClusteredMultiDatabaseManager databaseManager;

    ClusteredDbmsReconciler( ClusteredMultiDatabaseManager databaseManager, Config config, LogProvider logProvider, JobScheduler scheduler )
    {
        super( databaseManager, config, logProvider, scheduler );
        this.databaseManager = databaseManager;
    }

    @Override
    protected Stream<Function<DatabaseId,OperatorState>> prepareLifeCycleTransitionSteps( OperatorState currentState, DatabaseId databaseId,
            OperatorState desiredState )
    {
        if ( currentState == null )
        {
            // in clustering we always call create from stopped
            return Stream.concat( Stream.of( this::create ), prepareLifeCycleTransitionSteps( STOPPED, databaseId, desiredState ) );
        }
        else if ( currentState == STORE_COPYING && desiredState == DROPPED )
        {
            // No prepareDrop step needed here as the database will be stopped for store copying anyway
            return Stream.of( this::stop, this::drop );
        }
        else if ( currentState == STORE_COPYING && desiredState == STOPPED )
        {
            // Some Cluster components still need stopped when store copying.
            //   This will attempt to stop the kernel database again, but that should be idempotent.
            return Stream.of( this::stop );
        }
        else if ( currentState == STORE_COPYING && desiredState == STARTED )
        {
            return Stream.of( this::startAfterStoreCopy );
        }
        else if ( currentState == STARTED && desiredState == STORE_COPYING )
        {
            return Stream.of( this::stopBeforeStoreCopy );
        }
        else if ( currentState == STOPPED && desiredState == STORE_COPYING )
        {
            return Stream.of( this::start, this::stopBeforeStoreCopy );
        }

        return super.prepareLifeCycleTransitionSteps( currentState, databaseId, desiredState );
    }

    /* Operator Steps */
    private OperatorState startAfterStoreCopy( DatabaseId databaseId )
    {
        databaseManager.startDatabaseAfterStoreCopy( databaseId );
        return STARTED;
    }

    private OperatorState stopBeforeStoreCopy( DatabaseId databaseId )
    {
        databaseManager.stopDatabaseBeforeStoreCopy( databaseId );
        return STORE_COPYING;
    }
}
