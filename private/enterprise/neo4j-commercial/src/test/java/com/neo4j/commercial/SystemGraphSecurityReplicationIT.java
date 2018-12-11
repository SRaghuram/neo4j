/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.server.security.enterprise.auth.EnterpriseAuthAndUserManager;
import org.neo4j.server.security.enterprise.auth.EnterpriseUserManager;
import org.neo4j.server.security.enterprise.configuration.SecuritySettings;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.server.security.enterprise.configuration.SecuritySettings.SYSTEM_GRAPH_REALM_NAME;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class, SuppressOutputExtension.class} )
class SystemGraphSecurityReplicationIT
{
    private static final int DEFAULT_TIMEOUT_MS = 20000;

    @Inject
    private TestDirectory testDir;

    @Inject
    private DefaultFileSystemAbstraction fs;

    private CommercialCluster cluster;

    @BeforeEach
    void setup() throws Exception
    {
        Map<String,String> params = stringMap(
                GraphDatabaseSettings.auth_enabled.name(), Settings.TRUE,
                SecuritySettings.auth_provider.name(), SYSTEM_GRAPH_REALM_NAME
        );

        int noOfCoreMembers = 3;
        int noOfReadReplicas = 3;

        cluster = new CommercialCluster( testDir.absolutePath(), noOfCoreMembers, noOfReadReplicas, new SslHazelcastDiscoveryServiceFactory(), params,
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
            await( () -> getAllRoleNames( core.database() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> getAllRoleNames( replica.database() ).equals( expectedRoles ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    @Test
    void shouldReplicateNewUser() throws Exception
    {
        String username = "martin";
        String password = "235711";

        cluster.coreTx( ( db, tx ) ->
        {
            newUser( db, username, password );
            tx.success();
        } );

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            await( () -> userCanLogin( username, password, core.database() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }

        for ( ReadReplica replica : cluster.readReplicas() )
        {
            await( () -> userCanLogin( username, password, replica.database() ), DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    private void newUser( CoreGraphDatabase db, String username, String password )
    {
        try
        {
            userManager( db ).newUser( username, UTF8.encode( password ), false );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private Set<String> getAllRoleNames( GraphDatabaseFacade db )
    {
        try
        {
            return userManager( db ).getAllRoleNames();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private boolean userCanLogin( String username, String password, GraphDatabaseFacade db )
    {
        EnterpriseAuthManager authManager = authManager( db );
        AuthSubject subject = login( authManager, username, password );
        return AuthenticationResult.SUCCESS == subject.getAuthenticationResult();
    }

    private AuthSubject login( EnterpriseAuthManager authManager, String username, String password )
    {
        EnterpriseLoginContext loginContext;
        try
        {
            loginContext = authManager.login( authToken( username, password ) );
        }
        catch ( InvalidAuthTokenException e )
        {
            throw new RuntimeException( e );
        }
        return loginContext.subject();
    }

    private EnterpriseUserManager userManager( GraphDatabaseFacade db )
    {
        EnterpriseAuthManager enterpriseAuthManager = authManager( db );
        if ( enterpriseAuthManager instanceof EnterpriseAuthAndUserManager )
        {
            return ((EnterpriseAuthAndUserManager) enterpriseAuthManager).getUserManager();
        }
        throw new RuntimeException( "The configuration used does not have a user manager" );
    }

    private EnterpriseAuthManager authManager( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( EnterpriseAuthManager.class );
    }
}
