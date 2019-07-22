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
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.config.Setting;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
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
        int expectedNumberOfCoreMembers = 3;
        int expectedNumberOfReadReplicas = 2;

        Cluster cluster = clusterRule
                .withNumberOfCoreMembers( expectedNumberOfCoreMembers )
                .withNumberOfReadReplicas( expectedNumberOfReadReplicas )
                .startCluster();

        int actualNumberOfCoreMembers = cluster.coreMembers().size();
        assertThat( actualNumberOfCoreMembers, is( expectedNumberOfCoreMembers ) );
        int actualNumberOfReadReplicas = cluster.readReplicas().size();
        assertThat( actualNumberOfReadReplicas, is( expectedNumberOfReadReplicas ) );

        Set<Integer> portsUsed = gatherPortsUsed( cluster );

        // so many for core members, so many for read replicas, all unique
        assertThat( portsUsed, hasSize(
                actualNumberOfCoreMembers * NumberOfPortsUsedByCoreMember +
                        actualNumberOfReadReplicas * NumberOfPortsUsedByReadReplica ) );
    }

    private static Set<Integer> gatherPortsUsed( Cluster cluster )
    {
        Set<Integer> portsUsed = new HashSet<>();

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.discovery_listen_address ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.transaction_listen_address ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.raft_listen_address ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, OnlineBackupSettings.online_backup_listen_address ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, BoltConnector.listen_address ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, HttpConnector.listen_address ) );
        }

        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            portsUsed.add( getPortFromSetting( readReplica, CausalClusteringSettings.transaction_listen_address ) );
            portsUsed.add( getPortFromSetting( readReplica, OnlineBackupSettings.online_backup_listen_address ) );
            portsUsed.add( getPortFromSetting( readReplica, BoltConnector.listen_address ) );
            portsUsed.add( getPortFromSetting( readReplica, HttpConnector.listen_address ) );
        }
        return portsUsed;
    }

    private static int getPortFromSetting( ClusterMember coreClusterMember, Setting<SocketAddress> setting )
    {
        return coreClusterMember.settingValue( setting ).getPort();
    }
}
