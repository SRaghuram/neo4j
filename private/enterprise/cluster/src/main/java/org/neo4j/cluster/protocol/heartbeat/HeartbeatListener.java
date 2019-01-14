/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

import org.neo4j.cluster.InstanceId;

/**
 * Listener interface for heart beat. Implementations will receive
 * callbacks when instances in the cluster are considered failed or alive.
 */
public interface HeartbeatListener
{
    void failed( InstanceId server );

    void alive( InstanceId server );

    class Adapter implements HeartbeatListener
    {
        @Override
        public void failed( InstanceId server )
        {
        }

        @Override
        public void alive( InstanceId server )
        {
        }
    }
}
