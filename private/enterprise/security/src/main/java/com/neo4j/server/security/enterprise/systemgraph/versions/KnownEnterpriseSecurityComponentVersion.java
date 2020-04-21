/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.KnownSystemComponentVersion;

public abstract class KnownEnterpriseSecurityComponentVersion extends KnownSystemComponentVersion
{
    static final Label DATABASE_ALL_LABEL = Label.label( "DatabaseAll" );
    static final Label DATABASE_DEFAULT_LABEL = Label.label( "DatabaseDefault" );

    public static final Label USER_LABEL = Label.label( "User" );
    public static final Label ROLE_LABEL = Label.label( "Role" );
    static final Label PRIVILEGE_LABEL = Label.label( "Privilege" );

    public static final RelationshipType GRANTED = RelationshipType.withName( "GRANTED" );
    private static final RelationshipType USER_TO_ROLE = RelationshipType.withName( "HAS_ROLE" );
    public static final RelationshipType SCOPE = RelationshipType.withName( "SCOPE" );
    static final RelationshipType APPLIES_TO = RelationshipType.withName( "APPLIES_TO" );
    static final RelationshipType QUALIFIED = RelationshipType.withName( "QUALIFIED" );
    public static final RelationshipType FOR = RelationshipType.withName( "FOR" );

    private final boolean isCurrent;

    private List<Node> roleNodes = new ArrayList<>();

    KnownEnterpriseSecurityComponentVersion( int version, String description, Log log )
    {
        this( version, description, log, false );
    }

    KnownEnterpriseSecurityComponentVersion( int version, String description, Log log, boolean isCurrent )
    {
        super( EnterpriseSecurityGraphComponent.COMPONENT, version, description, log );
        this.isCurrent = isCurrent;
    }

    boolean componentNotInVersionNode( Transaction tx )
    {
        return getVersion( tx ) == NoEnterpriseComponentVersion.VERSION;
    }

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

    public boolean isCurrent()
    {
        return isCurrent;
    }

    public boolean isEmpty()
    {
        return roleNodes.isEmpty();
    }

    public void initializePrivileges( Transaction tx, List<String> roles, Map<String,Set<String>> roleUsers ) throws InvalidArgumentsException
    {
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

    Node newRole( Transaction tx, String roleName )
    {
        Node node = tx.createNode( ROLE_LABEL );
        node.setProperty( "name", roleName );
        roleNodes.add( node );
        return node;
    }
}
