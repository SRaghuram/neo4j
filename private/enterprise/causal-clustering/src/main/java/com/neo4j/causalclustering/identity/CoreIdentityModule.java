/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;

import java.util.UUID;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.StandaloneIdentityModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

public class CoreIdentityModule extends ClusteringIdentityModule
{
    /**
     * This fields type should be changed to ServerId over time
     */
    private final MemberId myself;

    /**
     * This field needs to be removed, a Map is needed here keyed by NamedDatabaseId or DatabaseId or UUID
     */
    // TODO: This field needs to be removed, a Map is needed here keyed by NamedDatabaseId or DatabaseId or UUID
    private final RaftMemberId tempRaftMemberId;

    public static CoreIdentityModule create( LogProvider logProvider, FileSystemAbstraction fs, Neo4jLayout layout, MemoryTracker memoryTracker,
            ClusterStateStorageFactory storageFactory )
    {
        var log = logProvider.getLog( StandaloneIdentityModule.class );
        var serverIdStorage = createServerIdStorage( fs, layout.serverIdFile(), memoryTracker );
        var serverId = readOrGenerate( serverIdStorage, log, ServerId.class, ServerId::of, UUID::randomUUID );
        return new CoreIdentityModule( new ClusteringServerId( serverId.getUuid() ) );
    }

    private CoreIdentityModule( MemberId memberId )
    {
        this.myself = memberId;
        this.tempRaftMemberId = new RaftMemberId( memberId.getUuid(), memberId );
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
    public RaftMemberId memberId( NamedDatabaseId namedDatabaseId )
    {
        return tempRaftMemberId;
    }
}
