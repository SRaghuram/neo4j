/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.version;

import java.util.Objects;

public class ClusterStateVersion
{
    private final int major;
    private final int minor;

    public ClusterStateVersion( int major, int minor )
    {
        this.major = major;
        this.minor = minor;
    }

    public int major()
    {
        return major;
    }

    public int minor()
    {
        return minor;
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
        ClusterStateVersion that = (ClusterStateVersion) o;
        return major == that.major &&
               minor == that.minor;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( major, minor );
    }

    @Override
    public String toString()
    {
        return "ClusterStateVersion{" +
               "major=" + major +
               ", minor=" + minor +
               '}';
    }
}
