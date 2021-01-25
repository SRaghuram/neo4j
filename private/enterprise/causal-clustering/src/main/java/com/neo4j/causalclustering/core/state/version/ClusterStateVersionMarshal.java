/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.version;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import org.neo4j.io.marshal.SafeStateMarshal;

public class ClusterStateVersionMarshal extends SafeStateMarshal<ClusterStateVersion>
{
    @Override
    public void marshal( ClusterStateVersion version, WritableChannel channel ) throws IOException
    {
        channel.putInt( version.major() );
        channel.putInt( version.minor() );
    }

    @Override
    protected ClusterStateVersion unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        var major = channel.getInt();
        var minor = channel.getInt();
        return new ClusterStateVersion( major, minor );
    }

    @Override
    public ClusterStateVersion startState()
    {
        return null;
    }

    @Override
    public long ordinal( ClusterStateVersion clusterStateVersion )
    {
        throw new UnsupportedOperationException( "Recovery for " + ClusterStateVersion.class.getSimpleName() + " storage is not required" );
    }
}
