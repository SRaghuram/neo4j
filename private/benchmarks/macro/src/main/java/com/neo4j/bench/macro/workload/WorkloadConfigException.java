/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.workload;

public class WorkloadConfigException extends RuntimeException
{
    private final WorkloadConfigError error;

    public WorkloadConfigException( WorkloadConfigError error )
    {
        this( error.message(), error );
    }

    public WorkloadConfigException( String message, WorkloadConfigError error )
    {
        this( message, error, null );
    }

    public WorkloadConfigException( String message, WorkloadConfigError error, Throwable cause )
    {
        super( message, cause );
        this.error = error;
    }

    public WorkloadConfigError error()
    {
        return error;
    }
}
