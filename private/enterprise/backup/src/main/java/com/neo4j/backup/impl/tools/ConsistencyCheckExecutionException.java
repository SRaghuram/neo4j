/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.tools;

public class ConsistencyCheckExecutionException extends Exception
{
    private final boolean consistencyCheckFailedToExecute;

    ConsistencyCheckExecutionException( String message, boolean consistencyCheckFailedToExecute )
    {
        this( message, null, consistencyCheckFailedToExecute );
    }

    ConsistencyCheckExecutionException( String message, Throwable cause, boolean consistencyCheckFailedToExecute )
    {
        super( message, cause );
        this.consistencyCheckFailedToExecute = consistencyCheckFailedToExecute;
    }

    public boolean executionFailure()
    {
        return consistencyCheckFailedToExecute;
    }
}
