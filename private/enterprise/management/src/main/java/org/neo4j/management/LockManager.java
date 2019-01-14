/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import java.util.List;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;
import org.neo4j.kernel.info.LockInfo;

@ManagementInterface( name = LockManager.NAME )
@Description( "Information about the Neo4j lock status" )
@Deprecated
public interface LockManager
{
    String NAME = "Locking";

    @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                  + "Neo4j has detected and averted (by throwing DeadlockDetectedException)." )
    long getNumberOfAvertedDeadlocks();

    @Description( "Information about all locks held by Neo4j" )
    List<LockInfo> getLocks();

    @Description( "Information about contended locks (locks where at least one thread is waiting) held by Neo4j. "
                  + "The parameter is used to get locks where threads have waited for at least the specified number "
                  + "of milliseconds, a value of 0 retrieves all contended locks." )
    List<LockInfo> getContendedLocks( long minWaitTime );
}
