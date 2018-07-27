/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, SuppressOutputExtension.class} )
class SecureClusterIT
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private DefaultFileSystemAbstraction fs;

    private CommercialCluster cluster;

    @AfterEach
    void cleanup()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldReplicateInSecureCluster() throws Exception
    {
        // given
        String sslPolicyName = "cluster";
        SslPolicyConfig policyConfig = new SslPolicyConfig( sslPolicyName );

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.ssl_policy.name(), sslPolicyName,
                policyConfig.base_directory.name(), "certificates/cluster"
        );
        Map<String,String> readReplicaParams = stringMap(
                CausalClusteringSettings.ssl_policy.name(), sslPolicyName,
                policyConfig.base_directory.name(), "certificates/cluster"
        );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        cluster = new CommercialCluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas,
                new SslHazelcastDiscoveryServiceFactory(), coreParams, emptyMap(), readReplicaParams,
                emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        // install the cryptographic objects for each core
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            File homeDir = cluster.getCoreMemberById( core.serverId() ).homeDir();
            int keyId = core.serverId();
            installKeyToInstance( homeDir, keyId );
        }

        // install the cryptographic objects for each read replica
        for ( ReadReplica replica : cluster.readReplicas() )
        {
            int keyId = replica.serverId() + noOfCoreMembers;
            File homeDir = cluster.getReadReplicaById( replica.serverId() ).homeDir();
            installKeyToInstance( homeDir, keyId );
        }

        // when
        cluster.start();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        Cluster.dataMatchesEventually( leader, cluster.coreMembers() );
        Cluster.dataMatchesEventually( leader, cluster.readReplicas() );
    }

    private void installKeyToInstance( File homeDir, int keyId ) throws IOException
    {
        File baseDir = new File( homeDir, "certificates/cluster" );
        fs.mkdirs( new File( baseDir, "trusted" ) );
        fs.mkdirs( new File( baseDir, "revoked" ) );

        SslResourceBuilder.caSignedKeyId( keyId ).trustSignedByCA().install( baseDir );
    }
}
