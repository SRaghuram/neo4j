/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

public interface HighAvailabilityMemberContext
{
    InstanceId getMyId();

    InstanceId getElectedMasterId();

    void setElectedMasterId( InstanceId electedMasterId );

    URI getAvailableHaMaster();

    void setAvailableHaMasterId( URI availableHaMasterId );

    boolean isSlaveOnly();
}
