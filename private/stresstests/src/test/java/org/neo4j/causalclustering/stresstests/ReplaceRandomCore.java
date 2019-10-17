/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.time.Duration.ofNanos;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.discovery_advertised_address;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.ONLY;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.time.Clocks.nanoClock;

class ReplaceRandomCore extends RepeatOnRandomCore
{
    private final Cluster<?> cluster;
    private final Log log;
    private final int akkaAlertLevel;
    private final int rollsBeforePause;
    private final Map<ClusterMember,AkkaReplicatedDataMonitor> akkaMonitors = new HashMap<>();

    private CoreClusterMember leader;
    private int rollCounter;

    ReplaceRandomCore( Control control, Resources resources )
    {
        super( control, resources );
        this.cluster = resources.cluster();
        this.log = resources.logProvider().getLog( getClass() );
        this.rollsBeforePause = cluster.coreMembers().size();
        this.akkaAlertLevel = cluster.coreMembers().size() + rollsBeforePause + 1;
    }

    @Override
    public void prepare()
    {
        for ( CoreClusterMember member : cluster.coreMembers() )
        {
            log.info( "Started " + member + " with " + member.id() + " at " + discoveryAddress( member ) );
        }
        cluster.coreMembers().forEach( core -> akkaMonitors.put( core, AkkaReplicatedDataMonitor.install( core, akkaAlertLevel, log ) ) );
    }

    @Override
    public void doWorkOnMember( ClusterMember oldMember ) throws Exception
    {
        log.info( "Stopping " + oldMember );
        oldMember.shutdown();
        akkaMonitors.remove( oldMember );

        CoreClusterMember newMember = cluster.newCoreMember();
        akkaMonitors.put( newMember, AkkaReplicatedDataMonitor.install( newMember, akkaAlertLevel, log ) );

        log.info( "Starting " + newMember );
        newMember.start();
        log.info( "Started " + newMember + " with " + newMember.id() + " at " + discoveryAddress( newMember ) );
        rollCounter++;

        awaitRaftMembershipThroughRaftMachine( newMember );
        checkLeaderThroughRaftMachine();

        checkCoreServerInfoThroughDiscovery();
        checkLeaderInfoThroughDiscovery();

        if ( rollCounter % rollsBeforePause == 0 )
        {
            log.info( "Pause for pruning" );
            waitForAkkaPruning();
        }
    }

    private AdvertisedSocketAddress discoveryAddress( CoreClusterMember newMember )
    {
        return newMember.config().get( discovery_advertised_address );
    }

    private void waitForAkkaPruning() throws InterruptedException
    {
        Optional<Duration> pruneDuration = sleepUntil( this::akkaIsPruned, ofSeconds( 60 ), logAkka() );
        pruneDuration.ifPresent( duration -> log.info( "Took %s seconds to prune", duration.getSeconds() ) );
    }

    private Runnable logAkka()
    {
        return () -> akkaMonitors.values().forEach( AkkaReplicatedDataMonitor::dump );
    }

    private boolean akkaIsPruned()
    {
        for ( AkkaReplicatedDataMonitor monitor : akkaMonitors.values() )
        {
            if ( monitor.maxSize() > startedCores().size() )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void validate()
    {
        for ( AkkaReplicatedDataMonitor monitor : akkaMonitors.values() )
        {
            monitor.dump();
            monitor.close();
        }
    }

    private void checkLeaderThroughRaftMachine() throws java.util.concurrent.TimeoutException
    {
        CoreClusterMember newLeader = cluster.awaitLeader();

        if ( newLeader != leader )
        {
            log.info( "New leader " + newLeader );
            leader = newLeader;
        }
    }

    private void awaitRaftMembershipThroughRaftMachine( CoreClusterMember core ) throws InterruptedException
    {
        log.info( "Waiting for raft membership of new member" );
        RaftMachine raft = core.database().getDependencyResolver().resolveDependency( RaftMachine.class, ONLY );
        assertEventually( members -> format( "Voting members %s do not contain %s", members, core.id() ),
                raft::votingMembers, hasItem( core.id() ), 10, MINUTES );
    }

    private void checkCoreServerInfoThroughDiscovery() throws InterruptedException
    {
        Collection<CoreClusterMember> cores = startedCores();

        // expected based on configuration
        Map<MemberId,CoreServerInfo> expected = new HashMap<>();
        for ( CoreClusterMember core : cores )
        {
            log.info( "Waiting for core server info to propagate to " + core );
            expected.put( core.id(), CoreServerInfo.from( core.config() ) );
        }

        // should be equal to actual extracted from discovery
        for ( CoreClusterMember core : cores )
        {
            TopologyService topologyService = topologyService( core );
            assertEventually( () -> topologyService.localCoreServers().members(), equalTo( expected ), 1, MINUTES );
        }
    }

    private void checkLeaderInfoThroughDiscovery() throws InterruptedException, TimeoutException
    {
        for ( CoreClusterMember core : startedCores() )
        {
            log.info( "Waiting for leader info to propagate to " + core );
            assertEventually( () -> getLeadersThroughTopology( core ), equalTo( singleton( cluster.awaitLeader().id() ) ), 1, MINUTES );
        }
    }

    private Set<MemberId> getLeadersThroughTopology( CoreClusterMember core )
    {
        return topologyService( core )
                .allCoreRoles()
                .entrySet()
                .stream()
                .filter( e -> e.getValue() == RoleInfo.LEADER )
                .map( Map.Entry::getKey )
                .collect( toSet() );
    }

    private Set<CoreClusterMember> startedCores()
    {
        return cluster.coreMembers().stream().filter( c -> !c.isShutdown() ).collect( toSet() );
    }

    private Optional<Duration> sleepUntil( BooleanSupplier endCondition, Duration actionEvery, Runnable action ) throws InterruptedException
    {
        long start = nanoClock().nanos();
        long nextAction = nanoClock().nanos();
        while ( !endCondition.getAsBoolean() )
        {
            if ( !control.keepGoing() )
            {
                return Optional.empty();
            }

            long now = nanoClock().nanos();
            if ( nextAction - now <= 0 )
            {
                action.run();
                nextAction = now + actionEvery.toNanos();
            }
            Thread.sleep( 1000 );
        }
        return Optional.of( ofNanos( nanoClock().nanos() - start ) );
    }

    private CoreTopologyService topologyService( CoreClusterMember core )
    {
        return core.database().getDependencyResolver().resolveDependency( CoreTopologyService.class, ONLY );
    }
}
