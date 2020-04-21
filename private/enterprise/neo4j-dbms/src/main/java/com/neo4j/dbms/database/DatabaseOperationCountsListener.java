/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.dbms.DatabaseStateChangedListener;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.database.DatabaseOperationCounts;

public class DatabaseOperationCountsListener implements DatabaseStateChangedListener
{
    private DatabaseOperationCounts.Counter counter;

    public DatabaseOperationCountsListener( DatabaseOperationCounts.Counter counter )
    {
        this.counter = counter;
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
        var previousFailed = previousState.hasFailed();
        var newFailed = newState.hasFailed();

        if ( !previousFailed && newFailed )
        {
            counter.increaseFailedCount();
        }
        else if ( previousFailed && !newFailed )
        {
            counter.increaseRecoveredCount();
        }
    }
}