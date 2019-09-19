/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.report;

import java.util.Objects;

import static java.lang.String.format;

public class MicroCoverageResult implements CsvRow
{
    static final String HEADER = "group,bench,test_count,change";

    public enum Change
    {
        BETTER,
        NO_CHANGE,
        MIXED,
        WORSE
    }

    private final String group;
    private final String bench;
    private final int testCount;
    private final Change change;

    MicroCoverageResult( String group,
                         String bench,
                         int testCount,
                         Change change )
    {
        this.group = group;
        this.bench = bench;
        this.testCount = testCount;
        this.change = change;
    }

    public String group()
    {
        return group;
    }

    public String bench()
    {
        return bench;
    }

    public int testCount()
    {
        return testCount;
    }

    public Change change()
    {
        return change;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MicroCoverageResult that = (MicroCoverageResult) o;
        return testCount == that.testCount &&
               Objects.equals( group, that.group ) &&
               Objects.equals( bench, that.bench ) &&
               change == that.change;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( group, bench, testCount, change );
    }

    @Override
    public String toString()
    {
        return row();
    }

    @Override
    public String row()
    {
        return format( "%s,%s,%s,%s",
                       group,
                       bench,
                       testCount,
                       change.name() );
    }
}
