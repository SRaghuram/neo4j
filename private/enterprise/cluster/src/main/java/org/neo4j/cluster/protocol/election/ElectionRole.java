/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.election;

/**
 * Role that an instance can have in a cluster.
 */
public class ElectionRole
{
    private String name;

    public ElectionRole( String name )
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ElectionRole that = (ElectionRole) o;

        return !(name != null ? !name.equals( that.name ) : that.name != null);

    }

    @Override
    public int hashCode()
    {
        return name != null ? name.hashCode() : 0;
    }
}
