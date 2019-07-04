/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

public class JobId
{
    private final String id;

    public JobId( String id )
    {
        this.id = id;
    }

    public String id()
    {
        return id;
    }
}
