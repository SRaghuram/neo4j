/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.INITIAL;
import static com.neo4j.dbms.OperatorState.UNKNOWN;

class DatabaseReconcilerEntry
{
    private final boolean failed;
    private final DatabaseState currentState;

    public static DatabaseReconcilerEntry initial( DatabaseId id )
    {
        return new DatabaseReconcilerEntry( new DatabaseState( id, INITIAL ), false );
    }

    public static DatabaseReconcilerEntry unknown( DatabaseId id )
    {
        return new DatabaseReconcilerEntry( new DatabaseState( id, UNKNOWN ), false );
    }

    DatabaseReconcilerEntry( DatabaseState currentState )
    {
        this( currentState, false );
    }

    private DatabaseReconcilerEntry( DatabaseState currentState, boolean failed )
    {
        this.currentState = currentState;
        this.failed = failed;
    }

    public DatabaseReconcilerEntry passed()
    {
        return new DatabaseReconcilerEntry( currentState, false );
    }

    public DatabaseReconcilerEntry failed()
    {
        return new DatabaseReconcilerEntry( currentState, true );
    }

    //TODO: @Martin, I believe that if we expose the current states of databases according to the reconciler through the database manager
    //  we can do away with the weird DatabaseContext#fail / DatabaseContext#hasFailed, which feel out of place. Thoughts?
    public boolean hasFailed()
    {
        return failed;
    }

    public DatabaseState state()
    {
        return currentState;
    }
}
