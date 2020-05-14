/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponent.LATEST_VERSION;

public class EnterpriseVersion_1_36 extends KnownEnterpriseSecurityComponentVersion
{
    private final Label databaseLabel = Label.label( "Database" );
    private final Label dbRoleLabel = Label.label( "DbRole" );
    private final RelationshipType hasDbRole = RelationshipType.withName( "HAS_DB_ROLE" );
    private final RelationshipType forRole = RelationshipType.withName( "FOR_ROLE" );
    private final RelationshipType forDatabase = RelationshipType.withName( "FOR_DATABASE" );
    private Config config;

    public EnterpriseVersion_1_36( Log log, Config config )
    {
        super( 1, "Neo4j 3.6", log );
        this.config = config;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, dbRoleLabel ) &&
               componentNotInVersionNode( tx );
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        throw unsupported();
    }

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        if ( !nodesWithLabelExist( tx, databaseLabel ) )
        {
            throw new IllegalStateException( "Security initialization cannot work without database initialization" );
        }
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        // Nothing to do, this graph model does not contain privileges
    }

    public void addRoleToUser( Transaction tx, Node role, String username ) throws InvalidArgumentsException
    {
        Node user = tx.findNode( USER_LABEL, "name", username );
        Node db = tx.findNode( databaseLabel, "name", config.get( GraphDatabaseSettings.default_database ) );

        if ( user == null )
        {
            throw logAndCreateException( String.format( "User %s did not exist", username ) );
        }

        Node dbRole = tx.createNode( dbRoleLabel );
        user.createRelationshipTo( dbRole, hasDbRole );
        dbRole.createRelationshipTo( role, forRole );
        dbRole.createRelationshipTo( db, forDatabase );
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest ) throws Exception
    {
        assert latest.version == LATEST_VERSION;
        setVersionProperty( tx, latest.version );
        List<Node> roles = tx.findNodes( ROLE_LABEL ).stream().collect( Collectors.toList() );
        latest.setUpDefaultPrivileges( tx );
        List<String> rolesToSetup = new ArrayList<>();
        Map<String,Set<String>> rolesUsers = new HashMap<>();
        Set<Node> dbRoles = new HashSet<>();
        for ( Node role : roles )
        {
            String roleName = (String) role.getProperty( "name" );
            HashSet<String> users = new HashSet<>();
            for ( Relationship r : role.getRelationships( Direction.INCOMING, forRole ) )
            {
                Node dbRole = r.getOtherNode( role );
                dbRoles.add( dbRole );
                for ( Relationship r2 : dbRole.getRelationships( Direction.INCOMING, hasDbRole ) )
                {
                    Node user = r2.getOtherNode( dbRole );
                    String username = (String) user.getProperty( "name" );
                    users.add( username );
                }
            }
            rolesToSetup.add( roleName );
            rolesUsers.put( roleName, users );
        }
        for ( Node dbRole : dbRoles )
        {
            for ( Relationship r : dbRole.getRelationships() )
            {
                r.delete();
            }
            dbRole.delete();
        }
        for ( Node role : roles )
        {
            role.delete();
        }
        latest.initializePrivileges( tx, rolesToSetup, rolesUsers );
    }
}
