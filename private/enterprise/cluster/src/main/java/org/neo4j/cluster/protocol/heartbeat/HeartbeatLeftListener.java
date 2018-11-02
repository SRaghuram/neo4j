/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HeartbeatLeftListener extends ClusterListener.Adapter
{
    private final HeartbeatContext heartbeatContext;
    private final Log log;

    public HeartbeatLeftListener( HeartbeatContext heartbeatContext, LogProvider logProvider )
    {
        this.heartbeatContext = heartbeatContext;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void leftCluster( InstanceId instanceId, URI member )
    {
        if ( heartbeatContext.isFailedBasedOnSuspicions( instanceId ) )
        {
            log.warn( "Instance " + instanceId + " (" + member + ") has left the cluster " +
                    "but is still treated as failed by HeartbeatContext" );

            heartbeatContext.serverLeftCluster( instanceId );
        }
    }
}
