/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;

/**
 * Interface that decouples information about a database instance from the instance itself.
 */
public interface HighAvailabilityMemberInfoProvider
{
    HighAvailabilityMemberState getHighAvailabilityMemberState();
}
