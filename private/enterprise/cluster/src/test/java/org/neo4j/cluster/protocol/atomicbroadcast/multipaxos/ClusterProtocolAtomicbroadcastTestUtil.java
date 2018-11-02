/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cluster.InstanceId;

public class ClusterProtocolAtomicbroadcastTestUtil
{
    private ClusterProtocolAtomicbroadcastTestUtil()
    {
    }

    public static Iterable<InstanceId> ids( int size )
    {
        List<InstanceId> ids = new ArrayList<>();
        for ( int i = 1; i <= size; i++ )
        {
            ids.add( new InstanceId( i ) );
        }
        return ids;
    }

    public static Map<InstanceId,URI> members( int size )
    {
        Map<InstanceId,URI> members = new HashMap<>();
        for ( int i = 1; i <= size; i++ )
        {
            members.put( new InstanceId( i ), URI.create( "http://localhost:" + (6000 + i) + "?serverId=" + i ) );
        }
        return members;
    }
}
