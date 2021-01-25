/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

public class Kaboom extends RuntimeException
{
    public Kaboom()
    {
        super( "This was entirely unexpected" );
    }

    public Kaboom( String message )
    {
        super( message );
    }

    public Kaboom( String message, Throwable cause )
    {
        super( message, cause );
    }
}
