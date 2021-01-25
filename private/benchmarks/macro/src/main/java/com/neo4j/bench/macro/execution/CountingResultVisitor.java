/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Result.ResultVisitor;

public class CountingResultVisitor implements ResultVisitor<RuntimeException>
{
    private int count;

    @Override
    public boolean visit( Result.ResultRow row ) throws RuntimeException
    {
        count++;
        // use row to avoid escape analysis
        if ( row == null )
        {
            throw new RuntimeException( "Row should not be null" );
        }
        return true;
    }

    public int count()
    {
        return count;
    }

    public void reset()
    {
        count = 0;
    }
}
