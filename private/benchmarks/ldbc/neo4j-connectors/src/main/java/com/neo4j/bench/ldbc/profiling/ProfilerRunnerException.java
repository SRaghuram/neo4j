/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.profiling;

public class ProfilerRunnerException extends Exception
{
    public ProfilerRunnerException( String message )
    {
        super( message );
    }

    public ProfilerRunnerException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
