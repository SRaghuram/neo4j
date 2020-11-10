/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.RoleRepository;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.systemgraph.CustomSecurityInitializer;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.logging.Log;
import org.neo4j.server.security.auth.ListSnapshot;
import org.neo4j.server.security.systemgraph.ComponentVersion;

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
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        throw unsupported();
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment ) throws UnsupportedOperationException
    {
        throw unsupported();
    }

    @Override
    public DatabaseSecurityCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        throw unsupported();
    }

    // INITIALIZATION

    @Override
    public void grantDefaultPrivileges( Transaction tx, Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        throw unsupported();
    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
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
        doMigrateRoles( tx, roleRepository );
        customSecurityInitializer.initialize( tx );
    }

    private void doMigrateRoles( Transaction tx, RoleRepository roleRepository ) throws Exception
    {
        ListSnapshot<RoleRecord> roleRepo = roleRepository.getSnapshot();

        for ( RoleRecord roleRecord : roleRepo.values() )
        {
            Node role = tx.createNode( ROLE_LABEL );
            role.setProperty( "name", roleRecord.name() );
            for ( String username : roleRecord.users() )
            {
                // connect user and role
                Node user = tx.findNode( USER_LABEL, "name", username );
                if ( user != null )
                {
                    user.createRelationshipTo( role, USER_TO_ROLE );
                }
            }
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
