/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.dbms.DatabaseStateChangedListener;
import com.neo4j.dbms.EnterpriseDatabaseState;

import java.util.Collection;

import org.neo4j.dbms.DatabaseState;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;

public class DatabaseOperationCountMonitorListener implements DatabaseStateChangedListener
{
    private DatabaseOperationCountMonitor monitor;

    public DatabaseOperationCountMonitorListener( DatabaseOperationCountMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
        // STORE_COPYING state will be treated as STARTED
        var previousOperatorState = previousState.operatorState().equals( STORE_COPYING ) ? STARTED : previousState.operatorState();
        // STORE_COPYING state will be treated as STARTED
        var newOperatorState = newState.operatorState().equals( STORE_COPYING ) ? STARTED : newState.operatorState();

        if ( !previousOperatorState.equals( newOperatorState ) )
        {
            // STORE_COPYING is STARTED (handled above)
            // INITIAL is not tracked
            // UNKNOWN is not tracked
            if ( DROPPED.equals( newOperatorState ) )
            {
                // This is not particularly good, reason is:
                //   if DB gets dropped as STARTED we goes through a stop transition
                if ( previousOperatorState != STOPPED )
                {
                    monitor.increaseStopCount();
                }
                monitor.increaseDropCount();
            }
            else if ( STOPPED.equals( newOperatorState ) )
            {
                monitor.increaseStopCount();
            }
            else if ( STARTED.equals( newOperatorState ) )
            {
                // This is not particularly good, reason is:
                //   if DB gets started from scratch it goes through a create transition
                if ( previousOperatorState == INITIAL )
                {
                    monitor.increaseCreateCount();
                }
                monitor.increaseStartCount();
            }
        }

        var previousFailed = previousState.hasFailed();
        var newFailed = newState.hasFailed();

        if ( !previousFailed && newFailed )
        {
            monitor.increaseFailedCount();
        }
        else if ( previousFailed && !newFailed )
        {
            monitor.increaseRecoveredCount();
        }
    }

    public void reset( Collection<EnterpriseDatabaseState> currentStates )
    {
        monitor.resetCounts();
        currentStates.forEach( state ->
                               {
                                   if ( state.hasFailed() )
                                   {
                                       monitor.increaseFailedCount();
                                   }
                                   switch ( state.operatorState() )
                                   {
                                   case STOPPED:
                                       monitor.increaseCreateCount();
                                       break;
                                   case STORE_COPYING:
                                       // STORE_COPYING is STARTED
                                   case STARTED:
                                       monitor.increaseCreateCount();
                                       monitor.increaseStartCount();
                                       break;
                                   default:
                                       // INITIAL is not tracked
                                       // UNKNOWN is not tracked
                                       // DROPPED will be reset to zero, drop count cannot be measured exactly:
                                       //   if a database 'foo' was dropped and recreated then old stats would show 1
                                       //   but after reset it would not count since 'currentStates' contains that 'foo' is started
                                       break;
                                   }
                               } );
    }
}
