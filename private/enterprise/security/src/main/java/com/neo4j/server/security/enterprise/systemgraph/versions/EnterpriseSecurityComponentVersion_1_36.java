/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.systemgraph.EnterpriseSecurityGraphComponentVersion.ENTERPRISE_SECURITY_36;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 3.6
 */
public class EnterpriseSecurityComponentVersion_1_36 extends KnownEnterpriseSecurityComponentVersion
{
    private final Label databaseLabel = Label.label( "Database" );
    private final Label dbRoleLabel = Label.label( "DbRole" );
    private final RelationshipType hasDbRole = RelationshipType.withName( "HAS_DB_ROLE" );
    private final RelationshipType forRole = RelationshipType.withName( "FOR_ROLE" );
    private final RelationshipType forDatabase = RelationshipType.withName( "FOR_DATABASE" );
    private final Config config;
    private final KnownEnterpriseSecurityComponentVersion previous;

    public EnterpriseSecurityComponentVersion_1_36( Log log, Config config, KnownEnterpriseSecurityComponentVersion previous )
    {
        super( ENTERPRISE_SECURITY_36, log );
        this.config = config;
        this.previous = previous;
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, dbRoleLabel ) &&
               componentNotInVersionNode( tx );
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

    @Override
    public void setUpDefaultPrivileges( Transaction tx, PrivilegeStore privilegeStore )
    {
        if ( !nodesWithLabelExist( tx, databaseLabel ) )
        {
            throw new IllegalStateException( "Security initialization cannot work without database initialization" );
        }
    }

    @Override
    public void grantDefaultPrivileges( Node role, String predefinedRole, PrivilegeStore privilegeStore )
    {
        // Nothing to do, this graph model does not contain privileges
    }

    @Override
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
    public void upgradeSecurityGraph( Transaction tx, int fromVersion ) throws Exception
    {
        if ( fromVersion < version )
        {
            // This will upgrade directly to the 4.0 version
            previous.upgradeSecurityGraph( tx, fromVersion );
        }
        else
        {
            List<Node> roles = tx.findNodes( ROLE_LABEL ).stream().collect( Collectors.toList() );
            log.info( String.format( "Upgrading security model from %s with %d roles", this.description, roles.size() ) );
            Set<Node> dbRoles = new HashSet<>();
            for ( Node role : roles )
            {
                for ( Relationship r : role.getRelationships( Direction.INCOMING, forRole ) )
                {
                    Node dbRole = r.getOtherNode( role );
                    dbRoles.add( dbRole );
                    List<Relationship> dbRolesToUsers = new ArrayList<>();
                    dbRole.getRelationships( Direction.INCOMING, hasDbRole ).forEach( dbRolesToUsers::add );
                    for ( Relationship dbRoleToUser : dbRolesToUsers )
                    {
                        Node user = dbRoleToUser.getOtherNode( dbRole );
                        user.createRelationshipTo( role, USER_TO_ROLE );
                    }
                }
            }
            for ( Node dbRole : dbRoles )
            {
                for ( Relationship r : dbRole.getRelationships() )
                {
                    r.delete();
                }
                dbRole.delete();
            }
        }
    }
}
