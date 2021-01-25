/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.util.Objects;

/**
 * JMH field and single value assigned to it
 */
public class ParameterValue implements Comparable<ParameterValue>
{
    private final String param;
    private final String value;

    public ParameterValue( String param, String value )
    {
        this.param = param;
        this.value = value;
    }

    public String param()
    {
        return param;
    }

    public String value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "(" + param + "," + value + ")";
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
        ParameterValue that = (ParameterValue) o;
        return Objects.equals( param, that.param ) &&
               Objects.equals( value, that.value );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( param, value );
    }

    @Override
    public int compareTo( ParameterValue other )
    {
        int comparison = this.param.compareTo( other.param );
        if ( comparison != 0 )
        {
            return comparison;
        }
        else
        {
            return this.value.compareTo( other.value );
        }
    }
}
