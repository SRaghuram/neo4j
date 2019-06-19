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
    private final OperatorState stepState;
    private final DatabaseManagementException stepError;
    private final OperatorState desiredState;

    ReconcilerStepResult( OperatorState stepState, DatabaseManagementException stepError, OperatorState desiredState )
    {
        this.stepState = stepState;
        this.stepError = stepError;
        this.desiredState = desiredState;
    }

    ReconcilerStepResult withState( OperatorState state )
    {
        return new ReconcilerStepResult( state, this.stepError, this.desiredState );
    }

    ReconcilerStepResult withError( DatabaseManagementException stepError )
    {
        return new ReconcilerStepResult( this.stepState, Exceptions.chain( this.stepError, stepError ), this.desiredState );
    }

    public OperatorState state()
    {
        return stepState;
    }

    public DatabaseManagementException error()
    {
        return stepError;
    }

    public OperatorState desiredState()
    {
        return desiredState;
    }
}
