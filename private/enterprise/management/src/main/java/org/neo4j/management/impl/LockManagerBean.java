/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management.impl;

import java.util.ArrayList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.management.LockManager;

@Service.Implementation( ManagementBeanProvider.class )
@Deprecated
public final class LockManagerBean extends ManagementBeanProvider
{
    public LockManagerBean()
    {
        super( LockManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new LockManagerImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new LockManagerImpl( management, true );
    }

    private static class LockManagerImpl extends Neo4jMBean implements LockManager
    {
        private final Locks lockManager;

        LockManagerImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.lockManager = lockManager( management );
        }

        private Locks lockManager( ManagementData management )
        {
            try
            {
                return management.resolveDependency( Locks.class );
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                return null;
            }
        }

        LockManagerImpl( ManagementData management, boolean mxBean )
        {
            super( management, mxBean );
            this.lockManager = lockManager( management );
        }

        @Override
        public long getNumberOfAvertedDeadlocks()
        {
            return -1L;
        }

        @Override
        public List<LockInfo> getLocks()
        {
            final List<LockInfo> locks = new ArrayList<>();
            lockManager.accept( ( resourceType, resourceId, description, waitTime, lockIdentityHashCode ) ->
                    locks.add( new LockInfo( resourceType.toString(), String.valueOf( resourceId ), description ) ) );
            return locks;
        }

        @Override
        public List<LockInfo> getContendedLocks( final long minWaitTime )
        {
            // Contended locks can no longer be found by the new lock manager, since that knowledge is not centralized.
            return getLocks();
        }
    }
}
