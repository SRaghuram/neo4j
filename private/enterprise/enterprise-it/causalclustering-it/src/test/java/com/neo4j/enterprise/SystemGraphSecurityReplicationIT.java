/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.read_replica.ReadReplica;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.security.SecurityHelpers.getAllRoleNames;
import static com.neo4j.security.SecurityHelpers.newUser;
import static com.neo4j.security.SecurityHelpers.userCanLogin;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
class SystemGraphSecurityReplicationIT
{
    private static final int DEFAULT_TIMEOUT_MS = 20_000;

    @Inject
    private TestDirectory testDir;

    private Cluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        Map<String,String> params = stringMap(
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME
        );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        DiscoveryServiceFactory discoveryServiceFactory = new AkkaDiscoveryServiceFactory();
        cluster = new Cluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, params,
                emptyMap(), params, emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();
    }

    @AfterEach
    void cleanup()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    void shouldHaveInitialRoles() throws Exception
    {
        Set<String> expectedRoles = new HashSet<>( asList( ADMIN, ARCHITECT, EDITOR, PUBLISHER, READER ) );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            await( () -> getAllRoleNames( core.defaultDatabase() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> getAllRoleNames( replica.defaultDatabase() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    @Test
    void shouldReplicateNewUser() throws Exception
    {
        String username = "martin";
        String password = "235711";

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( db, username, password );
            tx.commit();
        } );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            await( () -> userCanLogin( username, password, core.defaultDatabase() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> userCanLogin( username, password, replica.defaultDatabase() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }
}
