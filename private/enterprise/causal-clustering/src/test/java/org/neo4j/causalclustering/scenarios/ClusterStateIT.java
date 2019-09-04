/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.causalclustering.common.Cluster.dataMatchesEventually;

public class ClusterStateIT
{
    private final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME );

    private final FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );

    @Test
    public void shouldRecreateClusterStateIfStoreIsMissing() throws Throwable
    {
        // given
        FileSystemAbstraction fs = fileSystemRule.get();
        Cluster<?> cluster = clusterRule.startCluster();
        cluster.awaitLeader();

        cluster.coreTx( ( db, tx ) ->
        {
            SampleData.createData( db, 100 );
            tx.success();
        } );
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 5, TimeUnit.SECONDS );
        MemberId followerId = follower.id();
        // when
        follower.shutdown();
        fs.deleteRecursively( follower.databaseDirectory() );
        follower.start();

        // then
        assertNotEquals( "MemberId should have changed", followerId, follower.id() );
        dataMatchesEventually( follower, cluster.coreMembers() );
    }

    @Test
    public void shouldNotRecreateClusterStateOnRestart() throws Throwable
    {
        // given
        Cluster<?> cluster = clusterRule.startCluster();
        Set<MemberId> expected = memberIds( cluster );

        // when
        cluster.shutdown();
        cluster.start();

        // then
        assertEquals( "The memberIds of the cluster should not have changed!", expected, memberIds( cluster ) );
    }

    private Set<MemberId> memberIds( Cluster<?> cluster )
    {
        return cluster.coreMembers().stream().map( CoreClusterMember::id ).collect( Collectors.toSet() );
    }
}
