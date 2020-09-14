/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.CustomSecurityInitializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.util.Preconditions;

import static java.lang.String.format;
import static org.neo4j.server.security.systemgraph.ComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 3.5
 */
public class EnterpriseSecurityComponentVersion_0_35 extends KnownEnterpriseSecurityComponentVersion
{
    private final RoleRepository roleRepository;
    private CustomSecurityInitializer customSecurityInitializer;

    public EnterpriseSecurityComponentVersion_0_35( Log log, RoleRepository roleRepository, CustomSecurityInitializer customSecurityInitializer )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_35, log );
        this.roleRepository = roleRepository;
        this.customSecurityInitializer = customSecurityInitializer;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        if ( nodesWithLabelExist( tx, ROLE_LABEL ) || !componentNotInVersionNode( tx ) )
        {
            return false;
        }
        else
        {
            try
            {
                roleRepository.start();
                return roleRepository.numberOfRoles() > 0;
            }
            catch ( Exception e )
            {
                return false;
            }
        }
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        throw unsupported();
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        throw unsupported();
    }

    @Override
    public List<String> getPrivilegesAsCommands( Transaction tx, String databaseName, boolean saveUsers )
    {
        throw unsupported();
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        throw unsupported();
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest ) throws Exception
    {
        Preconditions.checkState( latest.version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION, latest.version ));
        roleRepository.start();
        log.info( String.format( "Upgrading security model from %s roles file with %d roles", this.description, roleRepository.numberOfRoles() ) );
        if ( roleRepository.getRoleByName( PredefinedRoles.PUBLIC ) != null )
        {
            throw logAndCreateException( "Automatic migration of users and roles into system graph failed because 'PUBLIC' role exists. " +
                                         "Please remove or rename that role and start again." );
        }
        Set<String> usernames = getAllNames( tx, USER_LABEL );
        if ( !validateUsersInRoles( usernames, roleRepository.getSnapshot().values() ) )
        {
            throw logAndCreateException( "Automatic migration of users and roles into system graph failed because repository files are inconsistent. " );
        }
        doMigrateRoles( tx, roleRepository, latest );
        setVersionProperty( tx, latest.version );
        customSecurityInitializer.initialize( tx );
    }

    private void doMigrateRoles( Transaction tx, RoleRepository roleRepository, KnownEnterpriseSecurityComponentVersion latest ) throws Exception
    {
        ListSnapshot<RoleRecord> roleRepo = roleRepository.getSnapshot();

        if ( !roleRepo.values().isEmpty() && isEmpty() )
        {
            Map<String,Set<String>> roleUsers = new HashMap<>();
            List<String> roles = new ArrayList<>();
            for ( RoleRecord roleRecord : roleRepo.values() )
            {
                String roleName = roleRecord.name();
                roles.add( roleName );
                roleUsers.put( roleName, roleRecord.users() );
            }
            latest.initializePrivileges( tx, roles, roleUsers );
        }

        // Log what happened to the security log
        String roleString = roleRepo.values().size() == 1 ? "role" : "roles";
        log.info( "Completed migration of %s %s into system graph.", Integer.toString( roleRepo.values().size() ), roleString );
    }

    private static boolean validateUsersInRoles( Set<String> usernameInUsers, List<RoleRecord> roles )
    {
        Set<String> usernamesInRoles = roles.stream().flatMap( rr -> rr.users().stream() ).collect( Collectors.toSet() );
        return usernameInUsers.containsAll( usernamesInRoles );
    }
}
