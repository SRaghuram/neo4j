/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import java.util.Objects;

public class Pid
{
    private final long pid;

    public Pid( long pid )
    {
        this.pid = pid;
    }

    public long get()
    {
        return pid;
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
        Pid pid1 = (Pid) o;
        return pid == pid1.pid;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( pid );
    }

    @Override
    public String toString()
    {
        return "Pid(" + pid + ")";
    }
}
