/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.server.security.systemgraph.KnownSystemComponentVersion;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.neo4j.server.security.systemgraph.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

public abstract class KnownEnterpriseSecurityComponentVersion extends KnownSystemComponentVersion
{
    static final Label DATABASE_ALL_LABEL = Label.label( "DatabaseAll" );
    static final Label DATABASE_DEFAULT_LABEL = Label.label( "DatabaseDefault" );
    static final Label DATABASE_LABEL = Label.label( "Database" );

    public static final Label USER_LABEL = Label.label( "User" );
    public static final Label ROLE_LABEL = Label.label( "Role" );
    static final Label PRIVILEGE_LABEL = Label.label( "Privilege" );

    public static final RelationshipType GRANTED = RelationshipType.withName( "GRANTED" );
    private static final RelationshipType USER_TO_ROLE = RelationshipType.withName( "HAS_ROLE" );
    public static final RelationshipType SCOPE = RelationshipType.withName( "SCOPE" );
    static final RelationshipType APPLIES_TO = RelationshipType.withName( "APPLIES_TO" );
    static final RelationshipType QUALIFIED = RelationshipType.withName( "QUALIFIED" );
    public static final RelationshipType FOR = RelationshipType.withName( "FOR" );

    private List<Node> roleNodes = new ArrayList<>();

    KnownEnterpriseSecurityComponentVersion( ComponentVersion componentVersion, Log log )
    {
        super( componentVersion, log );
    }

    boolean componentNotInVersionNode( Transaction tx )
    {
        return getVersion( tx ) == UNKNOWN_VERSION;
    }

    boolean supportsUpdateAction( PrivilegeAction action )
    {
        return false;
    }

    public abstract void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment )
            throws UnsupportedOperationException;

    public abstract void setUpDefaultPrivileges( Transaction tx );

    public abstract void assignDefaultPrivileges( Node role, String predefinedRole );

    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest ) throws Exception
    {
        throw unsupported();
    }

    public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        throw unsupported();
    }

    public Set<ResourcePrivilege> currentGetPrivilegeForRole( Transaction tx, String roleName )
    {
        throw unsupported();
    }

    // should only be called with lower-case database name
    public abstract DatabaseSecurityCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles );

    public boolean isEmpty()
    {
        return roleNodes.isEmpty();
    }

    public void initializePrivileges( Transaction tx, List<String> roles, Map<String,Set<String>> roleUsers ) throws InvalidArgumentsException
    {
        log.info( String.format( "Initializing security model with %d roles", roles.size() ) );

        // Create default privileges
        setUpDefaultPrivileges( tx );

        // Create the specified roles
        roles.forEach( roleName ->
        {
            Node role = newRole( tx, roleName );
            assignDefaultPrivileges( role, roleName );
        } );

        // Assign users to roles
        for ( Map.Entry<String,Set<String>> entry : roleUsers.entrySet() )
        {
            String roleName = entry.getKey();
            Node role = tx.findNode( ROLE_LABEL, "name", roleName );
            for ( String userName : entry.getValue() )
            {
                addRoleToUser( tx, role, userName );
                log.info( "Assigned %s role to user '%s'.", roleName, userName );
            }
        }
    }

    public void addRoleToUser( Transaction tx, Node role, String username ) throws InvalidArgumentsException
    {
        Node user = tx.findNode( USER_LABEL, "name", username );

        if ( user == null )
        {
            throw logAndCreateException( String.format( "User %s did not exist", username ) );
        }

        user.createRelationshipTo( role, USER_TO_ROLE );
    }

    public Set<String> getAllNames( Transaction tx, Label label )
    {
        ResourceIterator<Node> nodes = tx.findNodes( label );
        Set<String> usernames = nodes.stream().map( node -> (String) node.getProperty( "name" ) ).collect( Collectors.toSet() );
        nodes.close();
        return usernames;
    }

    public InvalidArgumentsException logAndCreateException( String message )
    {
        log.error( message );
        return new InvalidArgumentsException( message );
    }

    private Node newRole( Transaction tx, String roleName )
    {
        Node node = tx.createNode( ROLE_LABEL );
        node.setProperty( "name", roleName );
        roleNodes.add( node );
        return node;
    }

    Node createPublicRoleFromUpgrade( Transaction tx )
    {
        try
        {
            return newRole( tx, PUBLIC );
        }
        catch ( ConstraintViolationException e )
        {
            throw new IllegalStateException( "'PUBLIC' is a reserved role and must be dropped before upgrade can proceed", e );
        }
    }
}
