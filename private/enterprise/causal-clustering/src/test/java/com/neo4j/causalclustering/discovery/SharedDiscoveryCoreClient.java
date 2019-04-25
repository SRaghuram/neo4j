/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends AbstractCoreTopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final boolean refusesToBeLeader;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService,
            DiscoveryMember myself, LogProvider logProvider, Config config )
    {
        super( config, myself, logProvider, logProvider );
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
    }

    @Override
    public void start0() throws InterruptedException
    {
        sharedDiscoveryService.registerCoreMember( this );
        log.info( "Registered core server %s", myself );

        for ( DatabaseId databaseId : getDatabaseIds() )
        {
            sharedDiscoveryService.waitForClusterFormation( databaseId );
        }
        log.info( "Cluster formed" );
    }

    @Override
    public void stop0()
    {
        sharedDiscoveryService.unRegisterCoreMember( this );
        log.info( "Unregistered core server %s", myself );
    }

    @Override
    public boolean setClusterId( ClusterId clusterId, DatabaseId databaseId )
    {
        return sharedDiscoveryService.casClusterId( clusterId, databaseId );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return sharedDiscoveryService.allCoreServers();
    }

    @Override
    public CoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return sharedDiscoveryService.getCoreTopology( databaseId, this );
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
    public void setLeader0( LeaderInfo newLeader, DatabaseId databaseId )
    {
        sharedDiscoveryService.casLeaders( newLeader, databaseId );
    }

    @Override
    public void handleStepDown0( LeaderInfo steppingDown, DatabaseId databaseId )
    {
        sharedDiscoveryService.casLeaders( steppingDown, databaseId );
    }

    Set<DatabaseId> getDatabaseIds()
    {
        return myself.hostedDatabases();
    }

    CoreServerInfo getCoreServerInfo()
    {
        return new CoreServerInfo( config, getDatabaseIds() );
    }

    boolean refusesToBeLeader()
    {
        return refusesToBeLeader;
    }

    void onCoreTopologyChange( CoreTopology coreTopology )
    {
        log.info( "Notified of core topology change " + coreTopology );
        listenerService.notifyListeners( coreTopology );
    }

    @Override
    public String toString()
    {
        return "SharedDiscoveryCoreClient{" +
               "myself=" + myself +
               ", refusesToBeLeader=" + refusesToBeLeader +
               '}';
    }
}
