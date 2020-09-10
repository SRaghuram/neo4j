/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import static com.neo4j.backup.impl.OnlineBackupCommand.STATUS_CONSISTENCY_CHECK_ERROR;
import static com.neo4j.backup.impl.OnlineBackupCommand.STATUS_CONSISTENCY_CHECK_INCONSISTENT;

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

    int getExitCode()
    {
        return consistencyCheckFailedToExecute ? STATUS_CONSISTENCY_CHECK_ERROR : STATUS_CONSISTENCY_CHECK_INCONSISTENT;
    }
}
