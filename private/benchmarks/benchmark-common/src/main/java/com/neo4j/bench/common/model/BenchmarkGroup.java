/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.model;

import com.neo4j.bench.common.util.JsonUtil;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BenchmarkGroup
{
    public static final String NAME = "name";

    private final String name;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public BenchmarkGroup()
    {
        this( "-1" );
    }

    public BenchmarkGroup( String name )
    {
        this.name = requireNonNull( name );
    }

    public String name()
    {
        return name;
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
        BenchmarkGroup that = (BenchmarkGroup) o;
        return Objects.equals( name, that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name );
    }

    @Override
    public String toString()
    {
        //        return "BenchmarkGroup{name='" + name + "'}";
        // TODO JSON map key deserialization depends on this. Do not change until that dependency is removed/fixed.
        return JsonUtil.serializeJson( this );
    }
}
