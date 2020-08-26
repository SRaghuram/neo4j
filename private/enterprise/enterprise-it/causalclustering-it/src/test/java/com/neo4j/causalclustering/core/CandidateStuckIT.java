/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.function.Predicates.await;

@ClusterExtension
class CandidateStuckIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private int counter;

    @Test
    void shouldTryToReproduceIt() throws Exception
    {
        Cluster cluster = clusterFactory.createCluster( clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 )
                .withSharedCoreParam( CausalClusteringSettings.leader_failure_detection_window, "60s-61s" ) );
        cluster.start();

        var leader = cluster.awaitLeader();
        var databaseId = leader.databaseId();
        var followers = cluster.coreMembers().stream().filter( member -> !member.equals( leader ) ).collect( Collectors.toList() );
        for ( int i = 0; i < 100; i++ )
        {
            createNode( cluster );
        }
        var raftMachine = leader.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class );
        raftServerStop( leader );
        // clear HB responses
        triggerElection( leader );
        // force the step down
        triggerElection( leader );
        // start pre-election on and put into preVote - won't be successful since member is not reachable
        triggerElection( leader );
        // start pre-election on and put into preVote - won't be successful since one member is unreachable the other is not in preVote
        triggerElection( followers.get( 0 ) );
        // start pre-election on and put into preVote - will be successful since previous member is in preVote - also election start and win
        triggerElection( followers.get( 1 ) );
        raftServerStart();
        var newLeader = cluster.awaitLeader();
        createNode( cluster );
        createNode( cluster );
        raftServerStop( newLeader );
        var lastMember = cluster.coreMembers().stream().filter( member -> !member.equals( leader ) && !member.equals( newLeader ) ).findFirst();
        assertNotNull( lastMember );
        // fake delayed preVote response to force member to become candidate and start an election
        raftMachine.handle( new RaftMessages.PreVote.Response( followers.get( 0 ).raftMemberIdFor( databaseId ), raftMachine.term(), true ) );
        // ensure that previously started election fails and a new is started with bigger term
        triggerElection( leader );
        // clear HB responses
        triggerElection( newLeader );
        // force the step down
        triggerElection( newLeader );
        await( () -> hasNoLeader( cluster ), same -> same, 120000, MILLISECONDS );
        // cluster lost it's leader, test successful, let's see resolution
        // trigger leader loss on last guy standing
        lastMember.ifPresent( this::triggerElection );
        await( () -> hasNoLeader( cluster ), same -> !same, 120000, MILLISECONDS );
        raftServerStart();
    }

    private Boolean hasNoLeader( Cluster cluster )
    {
        for ( CoreClusterMember m : cluster.coreMembers() )
        {
            var managementService = m.managementService();
            if ( managementService == null )
            {
                continue;
            }
            try
            {
                var database = (GraphDatabaseFacade) managementService.database( DEFAULT_DATABASE_NAME );
                var role = database.getDependencyResolver().resolveDependency( RoleProvider.class ).currentRole();
                if ( role == Role.LEADER )
                {
                    return false;
                }
            }
            catch ( DatabaseNotFoundException e )
            {
                // ignored
            }
        }
        return true;
    }

    private CoreClusterMember raftServerStoppedOn;

    private void raftServerStop( CoreClusterMember member )
    {
        if ( raftServerStoppedOn != null )
        {
            raftServerStart();
        }
        var raftServer = CausalClusteringTestHelpers.raftServer( member );
        raftServer.stop();
        raftServerStoppedOn = member;
    }

    private void raftServerStart()
    {
        if ( raftServerStoppedOn != null )
        {
            var raftServer = CausalClusteringTestHelpers.raftServer( raftServerStoppedOn );
            raftServer.start();
            raftServerStoppedOn = null;
        }
    }

    private void triggerElection( CoreClusterMember member )
    {
        try
        {
            var raftMachine = member.resolveDependency( DEFAULT_DATABASE_NAME, RaftMachine.class );
            raftMachine.triggerElection();
            Thread.sleep( 40 );
        }
        catch ( IOException | InterruptedException e )
        {
            // ignored
        }
    }

    private void createNode( Cluster cluster ) throws Exception
    {
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode();
            node.setProperty( "prop", counter++ );
            tx.commit();
        } );
    }
}
