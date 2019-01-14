/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.LockingCompatibilityTestSuite;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.test.OtherThreadExecutor.WaitDetails;

public class ForsetiLocksTest extends LockingCompatibilityTestSuite
{
    @Override
    protected Locks createLockManager( Config config, Clock clock )
    {
        return new ForsetiLockManager( config, clock, ResourceTypes.values() );
    }

    @Override
    protected boolean isAwaitingLockAcquisition( WaitDetails details )
    {
        return details.isAt( ForsetiClient.class, "applyWaitStrategy" );
    }
}
