/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommandsProvider;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_0_35;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_10_43D2;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_1_36;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_2_40;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_3_41D1;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_4_41;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_5_42D4;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_6_42D6;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_7_42D7;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_8_42P1;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_9_43D1;
import com.neo4j.server.security.enterprise.systemgraph.versions.EnterpriseSecurityComponentVersion_Future;
import com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion;
import com.neo4j.server.security.enterprise.systemgraph.versions.NoEnterpriseSecurityComponentVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.dbms.database.KnownSystemComponentVersion;
import org.neo4j.dbms.database.KnownSystemComponentVersions;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;

import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.ROLE_LABEL;
import static com.neo4j.server.security.enterprise.systemgraph.versions.KnownEnterpriseSecurityComponentVersion.USER_LABEL;
import static org.neo4j.dbms.database.ComponentVersion.SECURITY_PRIVILEGE_COMPONENT;
import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;

/**
 * This component contains roles and privileges and is an enterprise-only component.
 * Each role is represented by a node with label :Role that is connected to zero or more users from the {@link UserSecurityGraphComponent}.
 * A privilege is represented of a relationship of type :GRANTED or :DENIED from a role node to a node with label (:Privilege),
 * which in turn is connected as below (where the database node is part of the DefaultSystemGraphComponent).
 *
 * (:Privilege)-[:SCOPE]->(s:Segment)-[:APPLIES_TO]->(:Resource), (s)-[:FOR]->(database), (s)-[:Qualified]->(qualifier)
 */
public class EnterpriseSecurityGraphComponent extends AbstractSystemGraphComponent implements DatabaseSecurityCommandsProvider
{
    private final UserRepository defaultAdminRepository;
    private final KnownSystemComponentVersions<KnownEnterpriseSecurityComponentVersion> knownSecurityComponentVersions =
            new KnownSystemComponentVersions<>( new NoEnterpriseSecurityComponentVersion() );
    private final CustomSecurityInitializer customSecurityInitializer;
    private final Log log;

    public EnterpriseSecurityGraphComponent( Log log, RoleRepository migrationRoleRepository, UserRepository defaultAdminRepository, Config config )
    {
        super( config );
        this.defaultAdminRepository = defaultAdminRepository;
        this.customSecurityInitializer = new CustomSecurityInitializer( config, log );
        this.log = log;

        KnownEnterpriseSecurityComponentVersion version0 =
                new EnterpriseSecurityComponentVersion_0_35( log, migrationRoleRepository, customSecurityInitializer );
        KnownEnterpriseSecurityComponentVersion version1 = new EnterpriseSecurityComponentVersion_1_36( log, config, version0 );
        KnownEnterpriseSecurityComponentVersion version2 = new EnterpriseSecurityComponentVersion_2_40( log, version1 );
        KnownEnterpriseSecurityComponentVersion version3 = new EnterpriseSecurityComponentVersion_3_41D1( log, version2 );
        KnownEnterpriseSecurityComponentVersion version4 = new EnterpriseSecurityComponentVersion_4_41( log, version3 );
        KnownEnterpriseSecurityComponentVersion version5 = new EnterpriseSecurityComponentVersion_5_42D4( log, version4 );
        KnownEnterpriseSecurityComponentVersion version6 = new EnterpriseSecurityComponentVersion_6_42D6( log, version5 );
        KnownEnterpriseSecurityComponentVersion version7 = new EnterpriseSecurityComponentVersion_7_42D7( log, version6 );
        KnownEnterpriseSecurityComponentVersion version8 = new EnterpriseSecurityComponentVersion_8_42P1( log, version7 );
        KnownEnterpriseSecurityComponentVersion version9 = new EnterpriseSecurityComponentVersion_9_43D1( log, version8 );
        KnownEnterpriseSecurityComponentVersion version10 = new EnterpriseSecurityComponentVersion_10_43D2( log, version9 );

        knownSecurityComponentVersions.add( version0 );
        knownSecurityComponentVersions.add( version1 );
        knownSecurityComponentVersions.add( version2 );
        knownSecurityComponentVersions.add( version3 );
        knownSecurityComponentVersions.add( version4 );
        knownSecurityComponentVersions.add( version5 );
        knownSecurityComponentVersions.add( version6 );
        knownSecurityComponentVersions.add( version7 );
        knownSecurityComponentVersions.add( version8 );
        knownSecurityComponentVersions.add( version9 );
        knownSecurityComponentVersions.add( version10 );
        knownSecurityComponentVersions.add( new EnterpriseSecurityComponentVersion_Future( log, knownSecurityComponentVersions.latestComponentVersion() ) );
    }

    @Override
    public String componentName()
    {
        return SECURITY_PRIVILEGE_COMPONENT;
    }

    @Override
    public Status detect( Transaction tx )
    {
        return knownSecurityComponentVersions.detectCurrentComponentVersion( tx ).getStatus();
    }

    @Override
    public void initializeSystemGraphModel( Transaction tx ) throws Exception
    {
        final KnownEnterpriseSecurityComponentVersion componentBeforeInit = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        log.info( "Initializing system graph model for component '%s' with version %d and status %s",
                SECURITY_PRIVILEGE_COMPONENT, componentBeforeInit.version, componentBeforeInit.getStatus() );
        initializeLatestSystemGraph( tx );
        KnownEnterpriseSecurityComponentVersion componentAfterInit = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        log.info( "After initialization of system graph model component '%s' have version %d and status %s",
                SECURITY_PRIVILEGE_COMPONENT, componentAfterInit.version, componentAfterInit.getStatus() );

    }

    @Override
    public void initializeSystemGraphConstraints( Transaction tx )
    {
        initializeSystemGraphConstraint( tx, ROLE_LABEL, "name" );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService system ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( system, tx ->
        {
            KnownEnterpriseSecurityComponentVersion currentVersion = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
            log.info( "Upgrading component '%s' with version %d and status %s to latest version",
                    SECURITY_PRIVILEGE_COMPONENT, currentVersion.version, currentVersion.getStatus() );

            if ( currentVersion.version == UNKNOWN_VERSION )
            {
                log.debug( "The current version does not have a security graph, doing a full initialization" );
                initializeLatestSystemGraph( tx );
            }
            else
            {
                if ( currentVersion.migrationSupported() )
                {
                    log.debug( "Upgrading security graph to latest version" );
                    knownSecurityComponentVersions.latestComponentVersion().upgradeSecurityGraph( tx, currentVersion.version );
                }
                else
                {
                    throw currentVersion.unsupported();
                }
            }
        } );
    }

    @Override
    protected void assertSystemGraphIntegrity( GraphDatabaseService system )
    {
        if ( config.get( GraphDatabaseInternalSettings.restrict_upgrade ) )
        {
            String upgradeUser = config.get( GraphDatabaseInternalSettings.upgrade_username );

            try ( Transaction tx = system.beginTx() )
            {
                Node node = tx.findNode( USER_LABEL, "name", upgradeUser );
                if ( node != null )
                {
                    throw new IllegalStateException( String.format( "The user specified by %s (%s) already exists in the system graph. " +
                                                                    "Change the username or delete the user before restricting upgrade.",
                                                                    GraphDatabaseInternalSettings.upgrade_username.name(),
                                                                    upgradeUser) );
                }
            }
        }
    }

    public void assertUpdateWithAction( Transaction tx, PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment )
            throws UnsupportedOperationException
    {
        KnownEnterpriseSecurityComponentVersion component = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        component.assertUpdateWithAction( action, specialDatabase, segment );
    }

    public KnownEnterpriseSecurityComponentVersion findSecurityGraphComponentVersion( String substring )
    {
        return knownSecurityComponentVersions.findComponentVersion( substring );
    }

    @Override
    public DatabaseSecurityCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        KnownEnterpriseSecurityComponentVersion component = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        return component.getBackupCommands( tx, databaseName.toLowerCase(), saveUsers, saveRoles );
    }

    public Set<ResourcePrivilege> getPrivilegesForRole( Transaction tx, String role )
    {
        KnownEnterpriseSecurityComponentVersion version = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        if ( !version.runtimeSupported() )
        {
            return Collections.emptySet();
        }
        return version.currentGetPrivilegeForRole( tx, role );
    }

    Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roles, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        KnownEnterpriseSecurityComponentVersion version = knownSecurityComponentVersions.detectCurrentComponentVersion( tx );
        if ( !version.runtimeSupported() )
        {
            return Collections.emptySet();
        }
        return version.getPrivilegeForRoles( tx, roles, privilegeCache );
    }

    private void initializeLatestSystemGraph( Transaction tx ) throws Exception
    {
        KnownEnterpriseSecurityComponentVersion latest = knownSecurityComponentVersions.latestComponentVersion();
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
        KnownEnterpriseSecurityComponentVersion latestComponent = knownSecurityComponentVersions.latestComponentVersion();
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
