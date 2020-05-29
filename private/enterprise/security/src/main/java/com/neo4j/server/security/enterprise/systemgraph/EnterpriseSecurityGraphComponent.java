/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_0_35;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_1_36;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_2_40;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_3_41d1;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_4_41;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseVersion_Future;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.server.security.enterprise.systemgraph.versions.NoEnterpriseComponentVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.KnownSystemComponentVersions;

import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.ROLE_LABEL;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.USER_LABEL;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

public class EnterpriseSecurityGraphComponent extends AbstractSystemGraphComponent
{
    private final UserRepository defaultAdminRepository;
    private final KnownSystemComponentVersions<KnownEnterpriseSecurityComponentVersion> knownSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoEnterpriseComponentVersion() );
    private final CustomSecurityInitializer customSecurityInitializer;
    private final Log log;
    public static final int LATEST_VERSION = 4;
    public static final String COMPONENT = "security-privileges";

    public EnterpriseSecurityGraphComponent( Log log, RoleRepository migrationRoleRepository, UserRepository defaultAdminRepository, Config config )
    {
        super( config );
        this.defaultAdminRepository = defaultAdminRepository;
        this.customSecurityInitializer = new CustomSecurityInitializer( config, log );
        this. log = log;
        knownSecurityComponentVersions.add( new EnterpriseVersion_0_35( log, migrationRoleRepository, customSecurityInitializer ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_1_36( log, config ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_2_40( log ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_3_41d1( log ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_4_41( log ) );
        knownSecurityComponentVersions.add( new EnterpriseVersion_Future( log, knownSecurityComponentVersions.latestSecurityGraphVersion() ) );
    }

    @Override
    public String component()
    {
        return COMPONENT;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx ).getStatus();
    }

    @Override
    public void initializeSystemGraphModel( Transaction tx ) throws Exception
    {
        final KnownEnterpriseSecurityComponentVersion componentBeforeInit = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        log.info( "Initializing system graph model for component '%s' with version %d and status %s",
                COMPONENT, componentBeforeInit.version, componentBeforeInit.getStatus() );
        initializeLatestSystemGraph( tx );
        KnownEnterpriseSecurityComponentVersion componentAfterInit = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        log.info( "After initialization of system graph model component '%s' have version %d and status %s",
                COMPONENT, componentAfterInit.version, componentAfterInit.getStatus() );

    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, ROLE_LABEL, "name" );
    }

    @Override
    public Optional<Exception> upgradeToCurrent( GraphDatabaseService system )
    {
        return SystemGraphComponent.executeWithFullAccess( system, tx ->
        {
            KnownEnterpriseSecurityComponentVersion currentVersion = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
            log.info( "Upgrading component '%s' with version %d and status %s to latest version",
                    COMPONENT, currentVersion.version, currentVersion.getStatus() );

            if ( currentVersion.version == NoEnterpriseComponentVersion.VERSION )
            {
                log.info( "The current version did not have any security graph, doing a full initialization" );
                initializeLatestSystemGraph( tx );
            }
            else
            {
                if ( currentVersion.migrationSupported() )
                {
                    log.info( "Upgrading security graph to latest version" );
                    currentVersion.upgradeSecurityGraph( tx, knownSecurityComponentVersions.latestSecurityGraphVersion() );
                }
                else
                {
                    throw currentVersion.unsupported();
                }
            }
        } );
    }

    public void assertUpdateWithAction( Transaction tx, PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        KnownEnterpriseSecurityComponentVersion component = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        component.assertUpdateWithAction( action, specialDatabase );
    }

    public KnownEnterpriseSecurityComponentVersion findSecurityGraphComponentVersion( String substring )
    {
        return knownSecurityComponentVersions.findSecurityGraphVersion( substring );
    }

    Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roles, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        KnownEnterpriseSecurityComponentVersion version = knownSecurityComponentVersions.detectCurrentSecurityGraphVersion( tx );
        if ( !version.runtimeSupported() )
        {
            return Collections.emptySet();
        }
        return version.getPrivilegeForRoles( tx, roles, privilegeCache );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion latest = knownSecurityComponentVersions.latestSecurityGraphVersion();
        Map<String,Set<String>> admins = new HashMap<>();
        admins.put( PredefinedRoles.ADMIN, Set.of( decideOnAdminUsername( tx ) ) );
        latest.initializePrivileges( tx, PredefinedRoles.roles, admins );
        customSecurityInitializer.initialize( tx );
    }

    /**
     * Tries to find an admin candidate among the existing users. Also supports the admin.ini file created by the neo4j-admin default-admin command which
     * supports upgrading from community to enterprise by selecting the future admin from all community users.
     */
    private String decideOnAdminUsername( Transaction tx ) throws Exception
    {
        String newAdmin = null;
        KnownEnterpriseSecurityComponentVersion latestComponent = knownSecurityComponentVersions.latestSecurityGraphVersion();
        Set<String> usernames = latestComponent.getAllNames( tx, USER_LABEL );

        // Try to determine who should be admin, by first checking the outcome of the SetDefaultAdmin command
        defaultAdminRepository.start();
        final int numberOfDefaultAdmins = defaultAdminRepository.numberOfUsers();
        if ( numberOfDefaultAdmins > 1 )
        {
            throw latestComponent.logAndCreateException( "No roles defined, and multiple users defined as default admin user. " + "Please use " +
                    "`neo4j-admin set-default-admin` to select a valid admin." );
        }
        else if ( numberOfDefaultAdmins == 1 )
        {
            newAdmin = defaultAdminRepository.getAllUsernames().iterator().next();
        }

        if ( newAdmin != null )
        {
            // We currently support only one default admin
            if ( !usernames.contains( newAdmin ) )
            {
                throw latestComponent.logAndCreateException( "No roles defined, and default admin user '" + newAdmin + "' does not exist. " +
                        "Please use `neo4j-admin set-default-admin` to select a valid admin." );
            }
            return newAdmin;
        }
        else if ( usernames.size() == 1 )
        {
            // If only a single user exists, make her an admin
            return usernames.iterator().next();
        }
        else if ( usernames.contains( INITIAL_USER_NAME ) )
        {
            // If the default neo4j user exists, make her an admin
            return INITIAL_USER_NAME;
        }
        else
        {
            throw latestComponent.logAndCreateException(
                    "No roles defined, and cannot determine which user should be admin. " + "Please use `neo4j-admin set-default-admin` to select an admin. " );
        }
    }
}
