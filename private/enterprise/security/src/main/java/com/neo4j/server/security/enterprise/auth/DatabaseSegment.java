/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

public class DatabaseSegment implements Segment
{
    private DatabaseSegment()
    {
    }

    @Override
    public String toString()
    {
        return "DatabaseSegment";
    }

    public static final DatabaseSegment ALL = new DatabaseSegment();
}
