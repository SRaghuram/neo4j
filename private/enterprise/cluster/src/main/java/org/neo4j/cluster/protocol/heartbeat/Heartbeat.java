/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.heartbeat;

/**
 * Heartbeat API. Register listeners that wants callback when instances
 * in the cluster are considered failed or alive.
 *
 * @see HeartbeatState
 * @see HeartbeatMessage
 */
public interface Heartbeat
{
    void addHeartbeatListener( HeartbeatListener listener );
    void removeHeartbeatListener( HeartbeatListener listener );
}
