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
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.LogProvider;

class SharedDiscoveryCoreClient extends AbstractCoreTopologyService implements Comparable<SharedDiscoveryCoreClient>
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final CoreServerInfo coreServerInfo;
    private final DatabaseId localDatabaseId;
    private final boolean refusesToBeLeader;

    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;

    SharedDiscoveryCoreClient( SharedDiscoveryService sharedDiscoveryService,
            DiscoveryMember myself, LogProvider logProvider, Config config )
    {
        super( config, myself, logProvider, logProvider );
        this.localDatabaseId = new DatabaseId( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.coreServerInfo = new CoreServerInfo( config, Set.of( localDatabaseId ) ); // todo: no db name like this!
        this.refusesToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
    }

    @Override
    public int compareTo( SharedDiscoveryCoreClient o )
    {
        return Optional.ofNullable( o ).map( c -> c.myself().getUuid().compareTo( this.myself().getUuid() ) ).orElse( -1 );
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
    public void setLeader0( LeaderInfo newLeader, DatabaseId databaseId )
    {
        sharedDiscoveryService.casLeaders( newLeader, databaseId );
    }

    @Override
    public void init0()
    {
        // nothing to do
    }

    @Override
    public void start0() throws InterruptedException
    {
        coreTopology = sharedDiscoveryService.getCoreTopology( this );
        readReplicaTopology = sharedDiscoveryService.getReadReplicaTopology();

        sharedDiscoveryService.registerCoreMember( this );
        log.info( "Registered core server %s", myself );

        sharedDiscoveryService.waitForClusterFormation();
        log.info( "Cluster formed" );
    }

    @Override
    public void stop0()
    {
        sharedDiscoveryService.unRegisterCoreMember( this );
        log.info( "Unregistered core server %s", myself );
    }

    @Override
    public void shutdown0()
    {
        // nothing to do
    }

    @Override
    public DatabaseId localDatabaseId()
    {
        return localDatabaseId;
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return coreTopology;
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return readReplicaTopology;
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        Optional<AdvertisedSocketAddress> coreAdvertisedSocketAddress = allCoreServers().find( upstream ).map( CoreServerInfo::getCatchupServer );
        if ( coreAdvertisedSocketAddress.isPresent() )
        {
            return coreAdvertisedSocketAddress.get();
        }
        return readReplicaTopology
                .find( upstream )
                .map( ReadReplicaInfo::getCatchupServer )
                .orElseThrow( () -> new CatchupAddressResolutionException( upstream ) );
    }

    @Override
    public void handleStepDown0( LeaderInfo steppingDown, DatabaseId databaseId )
    {
        sharedDiscoveryService.casLeaders( steppingDown, databaseId );
    }

    public CoreServerInfo getCoreServerInfo()
    {
        return coreServerInfo;
    }

    void onCoreTopologyChange( CoreTopology coreTopology )
    {
        log.info( "Notified of core topology change " + coreTopology );
        this.coreTopology = coreTopology;
        listenerService.notifyListeners( coreTopology );
    }

    void onReadReplicaTopologyChange( ReadReplicaTopology readReplicaTopology )
    {
        log.info( "Notified of read replica topology change " + readReplicaTopology );
        this.readReplicaTopology = readReplicaTopology;
    }

    public boolean refusesToBeLeader()
    {
        return refusesToBeLeader;
    }

    @Override
    public String toString()
    {
        return "SharedDiscoveryCoreClient{" + "myself=" + myself + ", coreServerInfo=" + coreServerInfo + ", refusesToBeLeader=" + refusesToBeLeader +
               ", localDBName='" + localDatabaseId + '\'' + ", coreTopology=" + coreTopology + '}';
    }
}
