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
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class CoreIdentityModule extends ClusteringIdentityModule
{
    /**
     * This field needs to be removed, a Map is needed here keyed by NamedDatabaseId or DatabaseId or Id
     */
    private final MemberId myself;

    public static CoreIdentityModule create( LogProvider logProvider, FileSystemAbstraction fs, File dataDir, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        var log = logProvider.getLog( StandaloneIdentityModule.class );
        var memberStorage = storageFactory.createMemberIdStorage();
        var memberId = readOrGenerate( memberStorage, log, MemberId.class, MemberId::of, UUID::randomUUID );

        // this is here temporary just to save the value of an already existing MemberId to ServerId
        var serverIdStorage = createServerIdStorage( fs, dataDir, memoryTracker );
        readOrGenerate( serverIdStorage, log, ServerId.class, ServerId::of, memberId::getUuid );

        return new CoreIdentityModule( memberId );
    }

    private CoreIdentityModule( MemberId memberId )
    {
        this.myself = memberId;
    }

    @Override
    public ServerId myself()
    {
        return myself;
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
}
