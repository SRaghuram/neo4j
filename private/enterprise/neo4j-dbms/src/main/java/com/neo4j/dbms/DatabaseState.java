/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

class DatabaseState
{
    private final DatabaseId databaseId;
    private final OperatorState operationalState;

    DatabaseState( DatabaseId databaseId, OperatorState operationalState )
    {
        this.databaseId = databaseId;
        this.operationalState = operationalState;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    OperatorState operationalState()
    {
        return operationalState;
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
        return Objects.equals( databaseId, that.databaseId ) && operationalState == that.operationalState;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, operationalState );
    }

    @Override
    public String toString()
    {
        return "DatabaseState{" + "databaseId=" + databaseId + ", operationalState=" + operationalState + '}';
    }
}
