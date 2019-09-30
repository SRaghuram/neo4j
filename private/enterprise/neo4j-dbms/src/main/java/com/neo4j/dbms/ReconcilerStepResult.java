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
    private final EnterpriseDatabaseState stepState;
    private final DatabaseManagementException stepError;
    private final EnterpriseDatabaseState desiredState;

    ReconcilerStepResult( EnterpriseDatabaseState stepState, DatabaseManagementException stepError, EnterpriseDatabaseState desiredState )
    {
        this.stepState = stepState;
        this.stepError = stepError;
        this.desiredState = desiredState;
    }

    ReconcilerStepResult withState( EnterpriseDatabaseState state )
    {
        return new ReconcilerStepResult( state, this.stepError, this.desiredState );
    }

    ReconcilerStepResult withError( DatabaseManagementException stepError )
    {
        return new ReconcilerStepResult( this.stepState, Exceptions.chain( this.stepError, stepError ), this.desiredState );
    }

    public EnterpriseDatabaseState state()
    {
        return stepState;
    }

    public DatabaseManagementException error()
    {
        return stepError;
    }

    EnterpriseDatabaseState desiredState()
    {
        return desiredState;
    }
}
