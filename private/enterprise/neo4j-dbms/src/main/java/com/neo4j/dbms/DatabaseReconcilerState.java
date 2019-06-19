/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

class DatabaseReconcilerState
{
    public static final DatabaseReconcilerState INITIAL = new DatabaseReconcilerState( null, false );

    private final boolean failed;
    private final OperatorState currentState;

    DatabaseReconcilerState( OperatorState currentState )
    {
        this( currentState, false );
    }

    private DatabaseReconcilerState( OperatorState currentState, boolean failed )
    {
        this.currentState = currentState;
        this.failed = failed;
    }

    public DatabaseReconcilerState passed()
    {
        return new DatabaseReconcilerState( currentState, false );
    }

    public DatabaseReconcilerState failed()
    {
        return new DatabaseReconcilerState( currentState, true );
    }

    //TODO: @Martin, I believe that if we expose the current states of databases according to the reconciler through the database manager
    //  we can do away with the weird DatabaseContext#fail / DatabaseContext#hasFailed, which feel out of place. Thoughts?
    public boolean hasFailed()
    {
        return failed;
    }

    public OperatorState state()
    {
        return currentState;
    }
}
