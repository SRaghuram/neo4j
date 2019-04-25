/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Reconciles the local operational state with the desired operational state.
 */
// TODO: Handling of dropped databases must reconsidered.
// TODO: Logging of transitions and the reasons for them.
public class ReconcilingDatabaseOperator
{
    private final Map<DatabaseId,OperatorState> currentStates;
    private final DatabaseManager databaseManager;

    public ReconcilingDatabaseOperator( DatabaseManager databaseManager, Map<DatabaseId,OperatorState> initialStates )
    {
        this.databaseManager = databaseManager;
        this.currentStates = new HashMap<>( initialStates );
    }

    void reconcile( List<Operator> operators )
    {
        HashMap<DatabaseId,OperatorState> combined = new HashMap<>();
        for ( Operator operator : operators )
        {
            combineInto( combined, operator.getDesired() );
        }
        combined.forEach( this::reconcile );
    }

    private void combineInto( Map<DatabaseId,OperatorState> combined, Map<DatabaseId,OperatorState> operator )
    {
        operator.forEach( ( databaseId, operatorState ) -> combined.compute( databaseId, ( ignored, current ) -> precedence( current, operatorState ) ) );
    }

    private OperatorState precedence( OperatorState first, OperatorState second )
    {
        if ( first == null )
        {
            return second;
        }
        else if ( first == DROPPED || second == DROPPED )
        {
            return DROPPED;
        }
        else if ( first == STOPPED || second == STOPPED )
        {
            return STOPPED;
        }
        else
        {
            return STARTED;
        }
    }

    private synchronized void reconcile( DatabaseId databaseId, OperatorState desiredState )
    {
        if ( currentStates.get( databaseId ) == null )
        {
            if ( desiredState == DROPPED )
            {
                currentStates.put( databaseId, DROPPED );
                return;
            }

            // TODO: Add error handling around the creation.
            databaseManager.createDatabase( databaseId );
            currentStates.put( databaseId, STOPPED );
        }

        if ( currentStates.get( databaseId ) != desiredState )
        {
            // TODO: Add error handling around the transitions.
            performDatabaseLifecycleTransition( databaseId, desiredState );
            currentStates.put( databaseId, desiredState );
        }
    }

    private void performDatabaseLifecycleTransition( DatabaseId databaseId, OperatorState desiredState )
    {
        switch ( desiredState )
        {
        case STARTED:
            databaseManager.startDatabase( databaseId );
            break;
        case STOPPED:
            databaseManager.stopDatabase( databaseId );
            break;
        case DROPPED:
            databaseManager.dropDatabase( databaseId );
            break;
        default:
            throw new IllegalStateException( "Unrecognized database state: " + desiredState );
        }
    }
}
