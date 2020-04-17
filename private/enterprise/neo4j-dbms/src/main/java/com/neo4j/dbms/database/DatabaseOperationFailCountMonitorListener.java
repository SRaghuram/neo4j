/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.dbms.DatabaseStateChangedListener;

import org.neo4j.dbms.DatabaseState;

public class DatabaseOperationFailCountMonitorListener implements DatabaseStateChangedListener
{
    private DatabaseOperationFailCountMonitor monitor;

    public DatabaseOperationFailCountMonitorListener( DatabaseOperationFailCountMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
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
}
