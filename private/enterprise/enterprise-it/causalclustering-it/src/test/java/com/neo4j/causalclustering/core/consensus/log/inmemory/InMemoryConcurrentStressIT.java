/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.inmemory;

import com.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class InMemoryConcurrentStressIT extends ConcurrentStressIT<InMemoryConcurrentStressIT.LifecycledInMemoryRaftLog>
{
    @Override
    public LifecycledInMemoryRaftLog createRaftLog( FileSystemAbstraction fsa, File dir )
    {
        return new LifecycledInMemoryRaftLog();
    }

    public static class LifecycledInMemoryRaftLog extends InMemoryRaftLog implements Lifecycle
    {

        @Override
        public void init()
        {

        }

        @Override
        public void start()
        {

        }

        @Override
        public void stop()
        {

        }

        @Override
        public void shutdown()
        {

        }
    }
}
