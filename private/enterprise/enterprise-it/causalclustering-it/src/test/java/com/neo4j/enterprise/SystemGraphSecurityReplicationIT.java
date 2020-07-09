/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.security.SecurityHelpers.getAllRoleNames;
import static com.neo4j.security.SecurityHelpers.newUser;
import static com.neo4j.security.SecurityHelpers.userCanLogin;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.function.Predicates.await;

@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class SystemGraphSecurityReplicationIT
{
    private static final int DEFAULT_TIMEOUT_MS = 20_000;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeEach
    void beforeEach() throws Exception
    {
        var params = Map.of(
                GraphDatabaseSettings.auth_enabled.name(), TRUE,
                SecuritySettings.authentication_providers.name(), SecuritySettings.NATIVE_REALM_NAME,
                SecuritySettings.authorization_providers.name(), SecuritySettings.NATIVE_REALM_NAME );

        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 )
                .withSharedCoreParams( params )
                .withSharedReadReplicaParams( params );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldHaveInitialRoles() throws Exception
    {
        var expectedRoles = Set.of( ADMIN, ARCHITECT, EDITOR, PUBLISHER, READER, PUBLIC );

        for ( var core : cluster.coreMembers() )
        {
            await( () -> getAllRoleNames( core.managementService() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }

        for ( var replica : cluster.readReplicas() )
        {
            await( () -> getAllRoleNames( replica.managementService() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }
    }

    @Test
    void shouldReplicateNewUser() throws Exception
    {
        var username = "martin";
        var password = "235711";

        cluster.systemTx( ( db, tx ) ->
        {
            newUser( tx, username, password );
            tx.commit();
        } );

        for ( var core : cluster.coreMembers() )
        {
            await( () -> userCanLogin( username, password, core.defaultDatabase() ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }

        for ( var replica : cluster.readReplicas() )
        {
            await( () -> userCanLogin( username, password, replica.defaultDatabase() ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
        }
    }
}
