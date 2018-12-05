/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.lock;

import com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiClient;
import com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.locking.CommunityLockAcquisitionTimeoutIT;
import org.neo4j.test.OtherThreadExecutor;

public class EnterpriseLockAcquisitionTimeoutIT extends CommunityLockAcquisitionTimeoutIT
{
    @Override
    protected Predicate<OtherThreadExecutor.WaitDetails> exclusiveLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( ForsetiClient.class, "acquireExclusive" );
    }

    @Override
    protected Predicate<OtherThreadExecutor.WaitDetails> sharedLockWaitingPredicate()
    {
        return waitDetails -> waitDetails.isAt( ForsetiClient.class, "acquireShared" );
    }

    @Override
    protected Locks getLockManager()
    {
        return getDependencyResolver().resolveDependency( ForsetiLockManager.class );
    }
}
