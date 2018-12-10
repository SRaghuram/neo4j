/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ClusterRuleIT
{
    private static final int NumberOfPortsUsedByCoreMember = 6;
    private static final int NumberOfPortsUsedByReadReplica = 4;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    @Test
    public void shouldAssignPortsToMembersAutomatically() throws Exception
    {
        Cluster<?> cluster = clusterRule.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 5 ).startCluster();

        int numberOfCoreMembers = cluster.coreMembers().size();
        assertThat( numberOfCoreMembers, is( 3 ) );
        int numberOfReadReplicas = cluster.readReplicas().size();
        assertThat( numberOfReadReplicas, is( 5 ) );

        Set<Integer> portsUsed = gatherPortsUsed( cluster );

        // so many for core members, so many for read replicas, all unique
        assertThat( portsUsed.size(), is(
                numberOfCoreMembers * NumberOfPortsUsedByCoreMember +
                        numberOfReadReplicas * NumberOfPortsUsedByReadReplica ) );
    }

    private Set<Integer> gatherPortsUsed( Cluster<?> cluster )
    {
        Set<Integer> portsUsed = new HashSet<>();

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.discovery_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.transaction_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.raft_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, OnlineBackupSettings.online_backup_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, new BoltConnector( "bolt" ).listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, new HttpConnector( "http" ).listen_address.name() ) );
        }

        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            portsUsed.add( getPortFromSetting( readReplica, CausalClusteringSettings.transaction_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, OnlineBackupSettings.online_backup_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, new BoltConnector( "bolt" ).listen_address.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, new HttpConnector( "http" ).listen_address.name() ) );
        }
        return portsUsed;
    }

    private int getPortFromSetting( ClusterMember coreClusterMember, String settingName )
    {
        String setting = coreClusterMember.settingValue( settingName );
        return Integer.valueOf( setting.split( ":" )[1] );
    }
}
