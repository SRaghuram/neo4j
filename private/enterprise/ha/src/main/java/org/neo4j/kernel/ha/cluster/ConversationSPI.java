/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;

/**
 * Conversation part of HA master SPI.
 * Allows to hide dependencies from conversation management.
 */
public interface ConversationSPI
{
    Locks.Client acquireClient();

    JobHandle scheduleRecurringJob( Group group, long interval, Runnable job );
}
