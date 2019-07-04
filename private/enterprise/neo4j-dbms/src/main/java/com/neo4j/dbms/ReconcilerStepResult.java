/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.internal.helpers.Exceptions;

class ReconcilerStepResult
{
    private final DatabaseState stepState;
    private final DatabaseManagementException stepError;
    private final DatabaseState desiredState;

    ReconcilerStepResult( DatabaseState stepState, DatabaseManagementException stepError, DatabaseState desiredState )
    {
        this.stepState = stepState;
        this.stepError = stepError;
        this.desiredState = desiredState;
    }

    ReconcilerStepResult withState( DatabaseState state )
    {
        return new ReconcilerStepResult( state, this.stepError, this.desiredState );
    }

    ReconcilerStepResult withError( DatabaseManagementException stepError )
    {
        return new ReconcilerStepResult( this.stepState, Exceptions.chain( this.stepError, stepError ), this.desiredState );
    }

    public DatabaseState state()
    {
        return stepState;
    }

    public DatabaseManagementException error()
    {
        return stepError;
    }

    DatabaseState desiredState()
    {
        return desiredState;
    }
}
