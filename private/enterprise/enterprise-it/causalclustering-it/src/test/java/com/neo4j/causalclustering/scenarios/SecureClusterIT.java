/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Level;
import org.neo4j.ssl.SslResourceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;
import static org.neo4j.graphdb.Label.label;

@ExtendWith( {SuppressOutputExtension.class, DefaultFileSystemExtension.class} )
@ClusterExtension
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class SecureClusterIT
{
    private static final String CERTIFICATES_DIR = "certificates/cluster";

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private ClusterFactory clusterFactory;

    @Test
    void shouldReplicateInSecureCluster() throws Exception
    {
        // given
        var sslPolicyConfig = SslPolicyConfig.forScope( CLUSTER );

        var coreParams = Map.of(
                CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString(),
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME,
                sslPolicyConfig.enabled.name(), TRUE,
                sslPolicyConfig.base_directory.name(), CERTIFICATES_DIR
        );
        var readReplicaParams = Map.of(
                CausalClusteringSettings.middleware_logging_level.name(), Level.DEBUG.toString(),
                sslPolicyConfig.enabled.name(), TRUE,
                sslPolicyConfig.base_directory.name(), CERTIFICATES_DIR
        );

        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedPrimaryParams( coreParams )
                .withSharedReadReplicaParams( readReplicaParams );

        var cluster = clusterFactory.createCluster( clusterConfig );

        // install the cryptographic objects for each core
        for ( var core : cluster.primaryMembers() )
        {
            var index = core.index();
            var homeDir = cluster.getPrimaryMemberByIndex( core.index() ).homePath();
            installKeyToInstance( homeDir, index );
        }

        // install the cryptographic objects for each read replica
        for ( var replica : cluster.readReplicas() )
        {
            var index = replica.index() + cluster.primaryMembers().size();
            var homeDir = cluster.getReadReplicaByIndex( replica.index() ).homePath();
            installKeyToInstance( homeDir, index );
        }

        // when
        cluster.start();

        var leader = cluster.primaryTx( ( db, tx ) ->
        {
            var node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );

        // then
        dataMatchesEventually( leader, cluster.primaryMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );
    }

    private void installKeyToInstance( Path homeDir, int index ) throws IOException
    {
        var baseDir = homeDir.resolve( CERTIFICATES_DIR );
        fs.mkdirs( baseDir.resolve( "trusted" ) );
        fs.mkdirs( baseDir.resolve( "revoked" ) );

        SslResourceBuilder.caSignedKeyId( index ).trustSignedByCA().install( baseDir );
    }
}
