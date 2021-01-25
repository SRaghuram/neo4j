/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import java.util.Arrays;
import java.util.Objects;

import org.neo4j.graphdb.Label;

import static java.lang.String.format;

public class LabelKeyDefinition
{
    private String labelName;
    private String[] keys;

    public LabelKeyDefinition()
    {
    }

    public LabelKeyDefinition( Label label, String... keys )
    {
        this.labelName = label.name();
        this.keys = keys;
    }

    public Label label()
    {
        return Label.label( labelName );
    }

    public String[] keys()
    {
        return keys;
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
        LabelKeyDefinition that = (LabelKeyDefinition) o;
        return Objects.equals( labelName, that.labelName ) &&
               Arrays.equals( keys, that.keys );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( labelName, Arrays.hashCode( keys ) );
    }

    @Override
    public String toString()
    {
        return format( "(:%s,%s)", labelName, Arrays.toString( keys ) );
    }
}
