/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.inmemory;

import com.neo4j.causalclustering.core.consensus.log.ConcurrentStressIT;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;

import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;

public class InMemoryConcurrentStressIT extends ConcurrentStressIT
{
    @Override
    public RaftLog createRaftLog( FileSystemAbstraction fsa, Path dir )
    {
        return new InMemoryRaftLog();
    }
}
