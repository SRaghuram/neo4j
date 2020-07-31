/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;

import java.io.File;
import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.StandaloneIdentityModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class ClusteringIdentityModuleImpl extends StandaloneIdentityModule implements ClusteringIdentityModule
{
    /**
     * This field needs to be removed, a Map is needed here keyed by NamedDatabaseId or DatabaseId or Id
     */
    private final MemberId myself;

    public static ClusteringIdentityModuleImpl create( LogProvider logProvider, FileSystemAbstraction fs, File dataDir, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        var log = logProvider.getLog( ClusteringIdentityModuleImpl.class );
        var storage = storageFactory.createMemberIdStorage();
        var memberId = readOrGenerate( storage, log, MemberId.class.getSimpleName(), MemberId::new, MemberId::getUuid, UUID::randomUUID );

        return new ClusteringIdentityModuleImpl( logProvider, createServerIdStorage( fs, dataDir, memoryTracker ), memberId );
    }

    private ClusteringIdentityModuleImpl( LogProvider logProvider, SimpleStorage<ServerId> storage, MemberId memberId )
    {
        super( logProvider, storage, memberId::getUuid );
        this.myself = memberId;
    }

    /**
     * This method is here for the time MemberId -> ServerId conversion is done.
     * It serves the purpose not to be forced to change everything at once
     * Either "myself" and with that "ServerId" is to be used, or "memberId( NamedDatabaseId )" when the call is in raft and a database context is present
     */
    @Deprecated
    @Override
    public MemberId memberId()
    {
        return myself;
    }

    @Override
    public MemberId memberId( NamedDatabaseId namedDatabaseId )
    {
        return myself;
    }

    /**
     * This method is here for the time MemberId -> ServerId conversion is done.
     * It serves the purpose not to be forced to change everything at once
     */
    @Deprecated
    public static MemberId fromServerId( ServerId serverId )
    {
        return new MemberId( serverId.getUuid() );
    }
}
