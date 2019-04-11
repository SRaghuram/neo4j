/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryReadReplicaClient extends SafeLifecycle implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final Config config;
    private final DiscoveryMember myself;
    private final Log log;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config, DiscoveryMember myself,
            LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.config = config;
        this.myself = myself;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start0()
    {
        sharedDiscoveryService.registerReadReplica( this );
        log.info( "Registered read replica member id: %s", myself() );
    }

    @Override
    public void stop0()
    {
        sharedDiscoveryService.unRegisterReadReplica( this );
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return sharedDiscoveryService.allCoreServers();
    }

    @Override
    public CoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return sharedDiscoveryService.getCoreTopology( databaseId, false );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return sharedDiscoveryService.allReadReplicas();
    }

    @Override
    public ReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return sharedDiscoveryService.getReadReplicaTopology( databaseId );
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        return sharedDiscoveryService.findCatchupAddress( upstream );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    @Override
    public MemberId myself()
    {
        return myself.id();
    }

    Set<DatabaseId> getDatabaseIds()
    {
        return myself.databaseIds();
    }

    ReadReplicaInfo getReadReplicaInfo()
    {
        return new ReadReplicaInfo( config, getDatabaseIds() );
    }

    @Override
    public String toString()
    {
        return "SharedDiscoveryReadReplicaClient{" +
               "myself=" + myself +
               '}';
    }
}
