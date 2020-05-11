/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.internal.helpers.collection.Iterables.single;

abstract class SupportedEnterpriseVersion extends KnownEnterpriseSecurityComponentVersion
{
    private Node matchNodePriv;
    private Node matchRelPriv;
    private Node writeNodePriv;
    private Node writeRelPriv;
    private Node defaultAccessPriv;
    private Node accessPriv;
    private Node tokenPriv;
    private Node indexPriv;
    private Node constraintPriv;
    private Node adminPriv;

    SupportedEnterpriseVersion( int version, String description, Log log )
    {
        super( version, description, log );
    }

    SupportedEnterpriseVersion( int version, String description, Log log, boolean isCurrent )
    {
        super( version, description, log, isCurrent );
    }

    UnsupportedOperationException unsupportedAction()
    {
        return new UnsupportedOperationException( "This operation is not supported while running in compatibility mode with version " + this.description );
    }

    abstract PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action );

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        // Check for DatabaseAll node to see if the default privileges were already setup
        if ( nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) )
        {
            return;
        }

        // Create a DatabaseAll node
        Node allDb = tx.createNode( DATABASE_ALL_LABEL );
        allDb.setProperty( "name", "*" );

        // Create a DatabaseDefault node
        Node defaultDb = tx.createNode( DATABASE_DEFAULT_LABEL );
        defaultDb.setProperty( "name", "DEFAULT" );

        // Create initial qualifier nodes
        Node labelQualifier = tx.createNode( Label.label( "LabelQualifierAll" ) );
        labelQualifier.setProperty( "type", "node" );
        labelQualifier.setProperty( "label", "*" );

        Node relQualifier = tx.createNode( Label.label( "RelationshipQualifierAll" ) );
        relQualifier.setProperty( "type", "relationship" );
        relQualifier.setProperty( "label", "*" );

        Node dbQualifier = tx.createNode( Label.label( "DatabaseQualifier" ) );
        dbQualifier.setProperty( "type", "database" );
        dbQualifier.setProperty( "label", "" );

        // Create initial segments nodes and connect them with DatabaseAll and qualifiers
        Label segmentLabel = Label.label( "Segment" );

        Node labelSegment = tx.createNode( segmentLabel );
        labelSegment.createRelationshipTo( labelQualifier, QUALIFIED );
        labelSegment.createRelationshipTo( allDb, FOR );

        Node relSegment = tx.createNode( segmentLabel );
        relSegment.createRelationshipTo( relQualifier, QUALIFIED );
        relSegment.createRelationshipTo( allDb, FOR );

        Node dbSegment = tx.createNode( segmentLabel );
        dbSegment.createRelationshipTo( dbQualifier, QUALIFIED );
        dbSegment.createRelationshipTo( allDb, FOR );

        Node defaultDbSegment = tx.createNode( segmentLabel );
        defaultDbSegment.createRelationshipTo( dbQualifier, QUALIFIED );
        defaultDbSegment.createRelationshipTo( defaultDb, FOR );

        // Create initial resource nodes
        Label resourceLabel = Label.label( "Resource" );

        Node graphResource = tx.createNode( resourceLabel );
        graphResource.setProperty( "type", Resource.Type.GRAPH.toString() );
        graphResource.setProperty( "arg1", "" );
        graphResource.setProperty( "arg2", "" );

        Node allPropResource = tx.createNode( resourceLabel );
        allPropResource.setProperty( "type", Resource.Type.ALL_PROPERTIES.toString() );
        allPropResource.setProperty( "arg1", "" );
        allPropResource.setProperty( "arg2", "" );

        Node dbResource = tx.createNode( resourceLabel );
        dbResource.setProperty( "type", Resource.Type.DATABASE.toString() );
        dbResource.setProperty( "arg1", "" );
        dbResource.setProperty( "arg2", "" );

        // Create initial privilege nodes and connect them with resources and segments
        matchNodePriv = tx.createNode( PRIVILEGE_LABEL );
        matchRelPriv = tx.createNode( PRIVILEGE_LABEL );
        writeNodePriv = tx.createNode( PRIVILEGE_LABEL );
        writeRelPriv = tx.createNode( PRIVILEGE_LABEL );
        defaultAccessPriv = tx.createNode( PRIVILEGE_LABEL );
        accessPriv = tx.createNode( PRIVILEGE_LABEL );
        tokenPriv = tx.createNode( PRIVILEGE_LABEL );
        adminPriv = tx.createNode( PRIVILEGE_LABEL );
        indexPriv = tx.createNode( PRIVILEGE_LABEL );
        constraintPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( matchNodePriv, PrivilegeAction.MATCH.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( matchRelPriv, PrivilegeAction.MATCH.toString(), relSegment, allPropResource );
        setupPrivilegeNode( writeNodePriv, PrivilegeAction.WRITE.toString(), labelSegment, graphResource );
        setupPrivilegeNode( writeRelPriv, PrivilegeAction.WRITE.toString(), relSegment, graphResource );
        setupPrivilegeNode( defaultAccessPriv, PrivilegeAction.ACCESS.toString(), defaultDbSegment, dbResource );
        setupPrivilegeNode( accessPriv, PrivilegeAction.ACCESS.toString(), dbSegment, dbResource );
        setupPrivilegeNode( tokenPriv, PrivilegeAction.TOKEN.toString(), dbSegment, dbResource );
        setupPrivilegeNode( indexPriv, PrivilegeAction.INDEX.toString(), dbSegment, dbResource );
        setupPrivilegeNode( constraintPriv, PrivilegeAction.CONSTRAINT.toString(), dbSegment, dbResource );
        setupPrivilegeNode( adminPriv, PrivilegeAction.ADMIN.toString(), dbSegment, dbResource );
    }

    @Override
    public void assignDefaultPrivileges( Node role, String predefinedRole )
    {
        switch ( predefinedRole )
        {
        case ADMIN:
            role.createRelationshipTo( adminPriv, GRANTED );

        case ARCHITECT:
            role.createRelationshipTo( constraintPriv, GRANTED );
            role.createRelationshipTo( indexPriv, GRANTED );

        case PUBLISHER:
            role.createRelationshipTo( tokenPriv, GRANTED );

        case EDITOR:
            // The segment part is ignored for this action
            role.createRelationshipTo( writeNodePriv, GRANTED );
            role.createRelationshipTo( writeRelPriv, GRANTED );

        case READER:
            role.createRelationshipTo( matchNodePriv, GRANTED );
            role.createRelationshipTo( matchRelPriv, GRANTED );
            role.createRelationshipTo( accessPriv, GRANTED );
            break; // All of the above cases are cumulative

        case PUBLIC:
            role.createRelationshipTo( defaultAccessPriv, GRANTED );

        default:
        }
    }

    private static void setupPrivilegeNode( Node privNode, String action, Node segmentNode, Node resourceNode )
    {
        privNode.setProperty( "action", action );
        privNode.createRelationshipTo( segmentNode, SCOPE );
        privNode.createRelationshipTo( resourceNode, APPLIES_TO );
    }

    Set<ResourcePrivilege> currentGetPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        Set<ResourcePrivilege> privileges = new HashSet<>();
        for ( String roleName : roleNames )
        {
            Set<ResourcePrivilege> rolePrivileges = currentGetPrivilegeForRole( tx, roleName );
            privilegeCache.put( roleName, rolePrivileges );
            privileges.addAll( rolePrivileges );
        }
        return privileges;
    }

    private Set<ResourcePrivilege> currentGetPrivilegeForRole( Transaction tx, String roleName )
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
}
