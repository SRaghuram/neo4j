/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.causalclustering.catchup.v4.metadata.DatabaseSecurityCommands;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.dbms.database.ComponentVersion;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static org.neo4j.internal.helpers.collection.Iterables.single;

public abstract class SupportedEnterpriseSecurityComponentVersion extends KnownEnterpriseSecurityComponentVersion
{
    static final String DB_PARAM = "database";

    protected SupportedEnterpriseSecurityComponentVersion( ComponentVersion componentVersion, Log log )
    {
        super( componentVersion, log );
    }

    UnsupportedOperationException unsupportedAction()
    {
        return new UnsupportedOperationException( "This operation is not supported while running in compatibility mode with version " + this.description );
    }

    public abstract PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action );

    Node mergeSegment( Transaction tx, Node dbNode, Node qualifierNode )
    {
        List<Node> segmentNodes = new ArrayList<>();
        qualifierNode.getRelationships( Direction.INCOMING, QUALIFIED ).forEach( r -> segmentNodes.add( r.getOtherNode( qualifierNode ) ) );
        for ( Node segment : segmentNodes )
        {
            List<Node> dbNodes = new ArrayList<>();
            segment.getRelationships( Direction.OUTGOING, FOR ).forEach( r -> dbNodes.add( r.getOtherNode( segment ) ) );
            for ( Node node : dbNodes )
            {
                if ( node.getId() == dbNode.getId() )
                {
                    // found correct segment
                    return segment;
                }
            }
        }

        // No segment was found, create one
        Node labelSegment = tx.createNode( SEGMENT_LABEL );
        labelSegment.createRelationshipTo( qualifierNode, QUALIFIED );
        labelSegment.createRelationshipTo( dbNode, FOR );
        return labelSegment;
    }

    Node mergeNode( Transaction tx, Label label, Map<String,Object> properties )
    {
        ResourceIterator<Node> nodes = tx.findNodes( label, properties );
        Node node;
        if ( nodes.hasNext() )
        {
            node = nodes.next();
        }
        else
        {
            node = tx.createNode( label );
            properties.forEach( node::setProperty );
        }
        nodes.close();
        return node;
    }

    static void setupPrivilegeNode( Node privNode, String action, Node segmentNode, Node resourceNode )
    {
        privNode.setProperty( "action", action );
        privNode.createRelationshipTo( segmentNode, SCOPE );
        privNode.createRelationshipTo( resourceNode, APPLIES_TO );
    }

    protected Set<ResourcePrivilege> currentGetPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        try
        {
            Set<ResourcePrivilege> privileges = new HashSet<>();
            for ( String roleName : roleNames )
            {
                Set<ResourcePrivilege> rolePrivileges = currentGetPrivilegeForRole( tx, roleName );
                rolePrivileges.addAll( getTemporaryPrivileges() );
                privilegeCache.put( roleName, rolePrivileges );
                privileges.addAll( rolePrivileges );
            }
            return privileges;
        }
        catch ( InvalidArgumentsException e )
        {
            throw new IllegalStateException( "Failed to authorize", e );
        }
    }

    Set<ResourcePrivilege> getTemporaryPrivileges() throws InvalidArgumentsException
    {
        return Collections.emptySet();
    }

    public Set<ResourcePrivilege> currentGetPrivilegeForRole( Transaction tx, String roleName )
    {
        Set<ResourcePrivilege> rolePrivileges = new HashSet<>();
        try
        {
            Node roleNode = tx.findNode( Label.label( "Role" ), "name", roleName );
            if ( roleNode != null )
            {
                roleNode.getRelationships( Direction.OUTGOING ).forEach( relToPriv ->
                {
                    try
                    {
                        final Node privilegeNode = relToPriv.getEndNode();
                        String grantOrDeny = relToPriv.getType().name();
                        String action = (String) privilegeNode.getProperty( "action" );

                        Node resourceNode = single(
                                privilegeNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "APPLIES_TO" ) ) ).getEndNode();

                        Node segmentNode = single(
                                privilegeNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "SCOPE" ) ) ).getEndNode();

                        Node dbNode = single( segmentNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "FOR" ) ) ).getEndNode();
                        String dbName = (String) dbNode.getProperty( "name" );

                        Node qualifierNode =
                                single( segmentNode.getRelationships( Direction.OUTGOING, RelationshipType.withName( "QUALIFIED" ) ) ).getEndNode();

                        ResourcePrivilege.GrantOrDeny privilegeType = ResourcePrivilege.GrantOrDeny.fromRelType( grantOrDeny );
                        PrivilegeBuilder privilegeBuilder = makePrivilegeBuilder( privilegeType, action );

                        privilegeBuilder.withinScope( qualifierNode ).onResource( resourceNode );

                        String dbLabel = single( dbNode.getLabels() ).name();
                        switch ( dbLabel )
                        {
                        case "Database":
                            privilegeBuilder.forDatabase( dbName );
                            break;
                        case "DatabaseAll":
                            privilegeBuilder.forAllDatabases();
                            break;
                        case "DatabaseDefault":
                            privilegeBuilder.forDefaultDatabase();
                            break;
                        case "DeletedDatabase":
                            //give up
                            return;
                        default:
                            throw new IllegalStateException(
                                    "Cannot have database node without either 'Database', 'DatabaseDefault' or 'DatabaseAll' labels: " + dbLabel );
                        }
                        rolePrivileges.addAll( privilegeBuilder.build() );
                    }
                    catch ( InvalidArgumentsException ie )
                    {
                        throw new IllegalStateException( "Failed to authorize", ie );
                    }
                } );
            }
        }
        catch ( NotFoundException n )
        {
            // Can occur if the role was dropped by another thread during the privilege lookup.
            // The behaviour should be the same as if the user did not have the role,
            // i.e. the role should not be added to the privilege map.
        }
        return rolePrivileges;
    }

    void grantExecuteProcedurePrivilegeTo( Transaction tx, Node roleNode )
    {
        // Create new privilege for execute procedures
        Node procQualifier = mergeNode( tx, Label.label( "ProcedureQualifierAll" ), Map.of( "type", "procedure", "label", "*" ) );
        grantExecutePrivilegeTo( tx, roleNode, procQualifier );
    }

    void grantExecuteFunctionPrivilegeTo( Transaction tx, Node roleNode )
    {
        // Create new privilege for execute functions
        Node funcQualifier = mergeNode( tx, Label.label( "FunctionQualifierAll" ), Map.of( "type", "function", "label", "*" ) );
        grantExecutePrivilegeTo( tx, roleNode, funcQualifier );
    }

    private void grantExecutePrivilegeTo( Transaction tx, Node roleNode, Node qualifier )
    {
        Node allDb = mergeNode( tx, DATABASE_ALL_LABEL, Map.of( "name", "*" ) );
        Node dbResource = mergeNode( tx, RESOURCE_LABEL, Map.of( "type", Resource.Type.DATABASE.toString(), "arg1", "", "arg2", "" ) );

        Node segment = mergeSegment( tx, allDb, qualifier );

        Node privilege = tx.createNode( PRIVILEGE_LABEL );
        setupPrivilegeNode( privilege, PrivilegeAction.EXECUTE.toString(), segment, dbResource );
        roleNode.createRelationshipTo( privilege, GRANTED );
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase, Segment segment ) throws UnsupportedOperationException
    {
        if ( !supportsUpdateAction( action ) )
        {
            throw unsupportedAction();
        }
    }

    public DatabaseSecurityCommands getBackupCommands( Transaction tx, String databaseName, boolean saveUsers, boolean saveRoles )
    {
        ArrayList<String> roleSetup = new ArrayList<>();
        ArrayList<String> userSetup = new ArrayList<>();

        roleSetup.add( String.format( "CREATE DATABASE $%s IF NOT EXISTS", DB_PARAM ) );
        Node databaseNode = tx.findNode( Label.label("Database"), "name", databaseName );
        if ( databaseNode != null && databaseNode.getProperty( "status" ).equals( "offline" ) )
        {
            roleSetup.add( String.format( "STOP DATABASE $%s", DB_PARAM ) );
        }

        List<String> roles;
        try ( ResourceIterator<Node> roleNodes = tx.findNodes( ROLE_LABEL ) )
        {
            roles = roleNodes.stream()
                             .map( r -> r.getProperty( "name" ).toString() )
                             .filter( r -> !r.equals( PUBLIC ) )
                             .collect( Collectors.toList() );
        }

        Map<String,ArrayList<String>> roleToPrivileges = getRelevantRolesAndPrivileges( tx, databaseName, roles, saveRoles );
        Set<String> relevantRoles = roleToPrivileges.keySet();

        if ( saveRoles )
        {
            relevantRoles.forEach( role -> roleSetup.addAll( roleToPrivileges.get( role ) ) );
        }

        if ( saveUsers )
        {
            userSetup.addAll( getUsersAsCommands( tx, relevantRoles, saveRoles ) );
        }

        return new DatabaseSecurityCommands( roleSetup, userSetup );
    }

    private Map<String,ArrayList<String>> getRelevantRolesAndPrivileges( Transaction tx, String databaseName, List<String> roles, boolean savePrivileges )
    {
        Map<String,ArrayList<String>> roleToPrivileges = new HashMap<>();
        String defaultDatabaseName = getDefaultDatabaseName( tx );

        for ( String role : roles )
        {
            Set<ResourcePrivilege> privileges = currentGetPrivilegeForRole( tx, role );
            Predicate<ResourcePrivilege> isRelevantPrivilege = p -> p.appliesToAll() ||
                                                                    p.getDbName().equals( databaseName ) ||
                                                                    databaseName.equals( defaultDatabaseName ) &&
                                                                    p.appliesToDefault();
            Set<ResourcePrivilege> relevantPrivileges = privileges.stream()
                                                                  .filter( isRelevantPrivilege )
                                                                  .filter( p -> !p.isDbmsPrivilege() )
                                                                  .collect( Collectors.toSet() );
            if ( !relevantPrivileges.isEmpty() )
            {
                roleToPrivileges.put( role, new ArrayList<>() );
            }

            if ( !relevantPrivileges.isEmpty() && savePrivileges )
            {
                roleToPrivileges.get( role ).add( String.format( "CREATE ROLE `%s` IF NOT EXISTS", role ) );

                Set<String> rolePrivileges = new HashSet<>();

                for ( ResourcePrivilege privilege : relevantPrivileges )
                {
                    try
                    {
                        rolePrivileges.addAll( privilege.asCommandFor( false, role, DB_PARAM ) );
                    }
                    catch ( RuntimeException e )
                    {
                        log.error( "Failed to write restore command for privilege '%s': %s", privilege.toString(), e.getMessage() );
                    }
                }

                roleToPrivileges.get( role ).addAll( rolePrivileges );
            }
        }

        return roleToPrivileges;
    }

    private List<String> getUsersAsCommands( Transaction tx, Set<String> relevantRoles, boolean withRoleGrants )
    {
        Map<String, String> users = new HashMap<>();
        Map<String, List<String>> userToRoles = new HashMap<>();

        for ( String role : relevantRoles )
        {
            Node roleNode = tx.findNode( Label.label( "Role" ), "name", role );
            roleNode.getRelationships(Direction.INCOMING, RelationshipType.withName( "HAS_ROLE" ) ).forEach( rel ->
            {
                Node startNode = rel.getStartNode();
                Map<String,Object> properties = startNode.getAllProperties();
                String username = (String) properties.get( "name" );
                String changeRequired = "CHANGE " + ((boolean) properties.get( "passwordChangeRequired" ) ? "" : "NOT ") + "REQUIRED";
                String setStatus = "SET STATUS " + ((boolean) properties.get( "suspended" ) ? "SUSPENDED" : "ACTIVE");
                try
                {
                    String maskedCredentials = SystemGraphCredential.maskSerialized( (String) properties.get( "credentials" ) );
                    users.put( username, String.format( "CREATE USER `%s` IF NOT EXISTS SET ENCRYPTED PASSWORD '%s' %s %s",
                                                        username, maskedCredentials, changeRequired, setStatus ) );

                    userToRoles.computeIfAbsent( username, u -> new ArrayList<>() );
                    if ( withRoleGrants )
                    {
                        userToRoles.get( username ).add( String.format( "GRANT ROLE `%s` TO `%s`", role, username ) );
                    }
                }
                catch ( InvalidArgumentsException e )
                {
                    log.error( "Failed to write restore command for user '%s': %s", username, e.getMessage() );
                }
            } );
        }

        List<String> commands = new ArrayList<>();
        users.forEach( ( user, command ) -> {
            commands.add( command );
            commands.addAll( userToRoles.get( user ) );
        });

        return commands;
    }

    private String getDefaultDatabaseName( Transaction tx )
    {
        String defaultDatabaseName = "";
        Node defaultDatabase = tx.findNode( DATABASE_LABEL, "default", true );
        if ( defaultDatabase != null )
        {
            defaultDatabaseName = defaultDatabase.getProperty( "name", "" ).toString();
        }
        return defaultDatabaseName;
    }
}
