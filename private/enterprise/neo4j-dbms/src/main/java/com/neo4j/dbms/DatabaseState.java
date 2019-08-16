/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.INITIAL;
import static com.neo4j.dbms.OperatorState.UNKNOWN;

class DatabaseState
{
    private final DatabaseId databaseId;
    private final OperatorState operationalState;
    private final boolean failed;

    public static DatabaseState initial( DatabaseId id )
    {
        return new DatabaseState( id, INITIAL, false );
    }

    public static DatabaseState unknown( DatabaseId id )
    {
        return new DatabaseState( id, UNKNOWN, false );
    }

    DatabaseState( DatabaseId databaseId, OperatorState operationalState )
    {
        this( databaseId, operationalState, false );
    }

    private DatabaseState( DatabaseId databaseId, OperatorState operationalState, boolean failed )
    {
        this.databaseId = databaseId;
        this.operationalState = operationalState;
        this.failed = failed;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    OperatorState operationalState()
    {
        return operationalState;
    }

    public DatabaseState passed()
    {
        return new DatabaseState( databaseId, operationalState, false );
    }

    public DatabaseState failed()
    {
        return new DatabaseState( databaseId, operationalState, true );
    }

    public boolean hasFailed()
    {
        return failed;
    }

    @Override
    public String toString()
    {
        return "DatabaseState{" + "databaseId=" + databaseId + ", operationalState=" + operationalState + ", failed=" + failed + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DatabaseState that = (DatabaseState) o;
        return failed == that.failed && Objects.equals( databaseId, that.databaseId ) && operationalState == that.operationalState;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, operationalState, failed );
    }
}
