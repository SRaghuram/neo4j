/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.RobustJobSchedulerWrapper;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.READ_REPLICAS_DB_NAME_MAP;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.READ_REPLICA_BOLT_ADDRESS_MAP;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.READ_REPLICA_MEMBER_ID_MAP;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.extractCatchupAddressesMap;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.getCoreTopology;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.getReadReplicaTopology;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.refreshGroups;

public class HazelcastClient extends SafeLifecycle implements TopologyService
{
    private final Log log;
    private final ClientConnectorAddresses connectorAddresses;
    private final RobustHazelcastWrapper hzInstance;
    private final RobustJobSchedulerWrapper scheduler;
    private final Config config;
    private final long timeToLive;
    private final long refreshPeriod;
    private final AdvertisedSocketAddress transactionSource;
    private final MemberId myself;
    private final List<String> groups;
    private final RetryStrategy topologyServiceRetryStrategy;
    private final String dbName;

    private JobHandle keepAliveJob;
    private JobHandle refreshTopologyJob;

    /* cached data updated during each refresh */
    private volatile CoreTopology coreTopology = CoreTopology.EMPTY;
    private volatile CoreTopology localCoreTopology = CoreTopology.EMPTY;
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile ReadReplicaTopology localReadReplicaTopology = ReadReplicaTopology.EMPTY;
    private volatile Map<MemberId,AdvertisedSocketAddress> catchupAddressMap = new HashMap<>();
    private volatile Map<MemberId,RoleInfo> coreRoles;

    public HazelcastClient( HazelcastConnector connector, JobScheduler scheduler, LogProvider logProvider,
            Config config, MemberId myself )
    {
        this.hzInstance = new RobustHazelcastWrapper( connector );
        this.config = config;
        this.log = logProvider.getLog( getClass() );
        this.scheduler = new RobustJobSchedulerWrapper( scheduler, log );
        this.connectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        this.transactionSource = config.get( CausalClusteringSettings.transaction_advertised_address );
        this.timeToLive = config.get( CausalClusteringSettings.read_replica_time_to_live ).toMillis();
        this.refreshPeriod = config.get( CausalClusteringSettings.cluster_topology_refresh ).toMillis();
        this.myself = myself;
        this.groups = config.get( CausalClusteringSettings.server_groups );
        this.topologyServiceRetryStrategy = resolveStrategy( refreshPeriod );
        this.dbName = config.get( CausalClusteringSettings.database );
        this.coreRoles = emptyMap();
    }

    private static RetryStrategy resolveStrategy( long refreshPeriodMillis )
    {
        int pollingFrequencyWithinRefreshWindow = 2;
        int numberOfRetries =
                pollingFrequencyWithinRefreshWindow + 1; // we want to have more retries at the given frequency than there is time in a refresh period
        long delayInMillis = refreshPeriodMillis / pollingFrequencyWithinRefreshWindow;
        return new RetryStrategy( delayInMillis, (long) numberOfRetries );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return coreRoles;
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }

    @Override
    public String localDBName()
    {
        return dbName;
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return coreTopology;
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return localCoreTopology;
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return readReplicaTopology;
    }

    @Override
    public  ReadReplicaTopology localReadReplicas()
    {
        return localReadReplicaTopology;
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId memberId ) throws CatchupAddressResolutionException
    {
        try
        {
            return topologyServiceRetryStrategy.apply( () -> catchupAddressMap.get( memberId ), Objects::nonNull );
        }
        catch ( TimeoutException e )
        {
            throw new CatchupAddressResolutionException( memberId );
        }
    }

    /**
     * Caches the topology so that the lookups are fast.
     */
    private void refreshTopology() throws HazelcastInstanceNotActiveException
    {
        CoreTopology newCoreTopology = hzInstance.apply( hz -> getCoreTopology( hz, config, log ) );
        coreTopology = newCoreTopology;
        localCoreTopology = newCoreTopology.filterTopologyByDb( dbName );

        ReadReplicaTopology newReadReplicaTopology = hzInstance.apply( hz -> getReadReplicaTopology( hz, log ) );
        readReplicaTopology = newReadReplicaTopology;
        localReadReplicaTopology = newReadReplicaTopology.filterTopologyByDb( dbName );

        catchupAddressMap = extractCatchupAddressesMap( localCoreServers(), localReadReplicas() );
    }

    private void refreshRoles() throws HazelcastInstanceNotActiveException
    {
        coreRoles = hzInstance.apply(hz -> HazelcastClusterTopology.getCoreRoles( hz, allCoreServers().members().keySet() ) );
    }

    @Override
    public void init0()
    {
        // nothing to do
    }

    @Override
    public void start0()
    {
        keepAliveJob = scheduler.scheduleRecurring( Group.HZ_TOPOLOGY_KEEP_ALIVE, timeToLive / 3, this::keepReadReplicaAlive );
        refreshTopologyJob = scheduler.scheduleRecurring( Group.HZ_TOPOLOGY_REFRESH, refreshPeriod, () -> {
            this.refreshTopology();
            this.refreshRoles();
        } );
    }

    @Override
    public void stop0()
    {
        keepAliveJob.cancel( true );
        refreshTopologyJob.cancel( true );
        disconnectFromCore();
    }

    @Override
    public void shutdown0()
    {
        // nothing to do
    }

    private void disconnectFromCore()
    {
        try
        {
            String uuid = hzInstance.apply( hzInstance -> hzInstance.getLocalEndpoint().getUuid() );
            hzInstance.apply( hz -> hz.getMap( READ_REPLICA_BOLT_ADDRESS_MAP ).remove( uuid ) );
            hzInstance.shutdown();
        }
        catch ( Throwable e )
        {
            // Hazelcast is not able to stop correctly sometimes and throws a bunch of different exceptions
            // let's simply log the current problem but go on with our shutdown
            log.warn( "Unable to shutdown hazelcast cleanly", e );
        }
    }

    private void keepReadReplicaAlive() throws HazelcastInstanceNotActiveException
    {
        hzInstance.perform( hazelcastInstance ->
        {
            String hzId = hazelcastInstance.getLocalEndpoint().getUuid();
            String addresses = connectorAddresses.toString();
            log.debug( "Adding read replica into cluster (%s -> %s)", hzId, addresses );

            hazelcastInstance.getMap( READ_REPLICAS_DB_NAME_MAP ).put( hzId, dbName, timeToLive, MILLISECONDS);

            hazelcastInstance.getMap( READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP ).put( hzId, transactionSource.toString(), timeToLive, MILLISECONDS );

            hazelcastInstance.getMap( READ_REPLICA_MEMBER_ID_MAP ).put( hzId, myself.getUuid().toString(), timeToLive, MILLISECONDS );

            refreshGroups( hazelcastInstance, hzId, groups );

            // this needs to be last as when we read from it in HazelcastClusterTopology.readReplicas
            // we assume that all the other maps have been populated if an entry exists in this one
            hazelcastInstance.getMap( READ_REPLICA_BOLT_ADDRESS_MAP ).put( hzId, addresses, timeToLive, MILLISECONDS );
        } );
    }
}
