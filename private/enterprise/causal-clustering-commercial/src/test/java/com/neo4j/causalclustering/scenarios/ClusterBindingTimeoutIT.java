/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.scenarios.DiscoveryServiceType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.AKKA;
import static com.neo4j.causalclustering.scenarios.CommercialDiscoveryServiceType.HAZELCAST;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class ClusterBindingTimeoutIT
{
    private final int numCoreMembers = 3;
    private final ClusterRule clusterRule;
    private final FileSystemRule fileSystemRule;

    @Rule
    public RuleChain ruleChain;

    private Cluster<?> cluster;

    public ClusterBindingTimeoutIT( DiscoveryServiceType discovery )
    {
        clusterRule = new ClusterRule()
                .withDiscoveryServiceType( discovery )
                .withNumberOfCoreMembers( numCoreMembers )
                .withNumberOfReadReplicas( 0 );
        fileSystemRule = new DefaultFileSystemRule();
        ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );
    }

    @Parameterized.Parameters( name = "discovery-{0}" )
    public static List<DiscoveryServiceType> params()
    {
        //No Shared yet as shared discovery doesn't actually use initial_discovery_members so can't be prevented from binding successfully
        return asList( AKKA, HAZELCAST );
    }

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldTimeoutAndThrowOnUnsuccessfulBinding() throws Exception
    {
        CoreClusterMember confusedCore = cluster.addCoreMemberWithId( numCoreMembers,
                MapUtil.stringMap( CausalClusteringSettings.initial_discovery_members.name(), "foo:9191,bar:9191,baz:9191",
                        CausalClusteringSettings.cluster_binding_timeout.name(), "5s" ), Collections.emptyMap() );

        try
        {
            confusedCore.start();
            fail();
        }
        catch ( Exception e )
        {
            if ( !Exceptions.contains( e, TimeoutException.class ) )
            {
                throw e;
            }
        }
        assertEventually( "Confused Core has shutdown", confusedCore::isShutdown, is( true ), 10, TimeUnit.SECONDS );
    }
}
