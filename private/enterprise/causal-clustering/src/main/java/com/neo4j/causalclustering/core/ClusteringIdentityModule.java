/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.StandaloneIdentityModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class ClusteringIdentityModule extends StandaloneIdentityModule
{
    private final MemberId myself;

    static ClusteringIdentityModule create( LogProvider logProvider, FileSystemAbstraction fs, File dataDir, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        var log = logProvider.getLog( ClusteringIdentityModule.class );
        var storage = storageFactory.createMemberIdStorage();
        var memberId = readOrGenerate( storage, log, MemberId.class.getSimpleName(), MemberId::new, UUID::randomUUID );

        return new ClusteringIdentityModule( logProvider, createServerIdStorage( fs, dataDir, memoryTracker ), memberId );
    }

    private ClusteringIdentityModule( LogProvider logProvider, SimpleStorage<ServerId> storage, MemberId memberId )
    {
        super( logProvider, storage, ServerId::new, memberId::getUuid );
        this.myself = memberId;
    }

    public MemberId memberId()
    {
        return myself;
    }
}
