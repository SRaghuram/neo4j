/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;

import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.logging.Log;

import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLIC;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;

public class EnterpriseVersion_2_40 extends SupportedEnterpriseVersion
{
    private Node traverseNodePriv;
    private Node traverserRelPriv;
    private Node readNodePriv;
    private Node readRelPriv;
    private Node writeNodePriv;
    private Node writeRelPriv;
    private Node accessPriv;
    private Node tokenPriv;
    private Node schemaPriv;
    private Node adminPriv;

    public EnterpriseVersion_2_40( Log log )
    {
        super( 2, "Neo4j 4.0", log );
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, EnterpriseVersion_3_41d1.DATABASE_ALL_LABEL ) &&
               !nodesWithLabelExist( tx, EnterpriseVersion_3_41d1.DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public boolean runtimeSupported()
    {
        return true;
    }

    private static void setupPrivilegeNode( Node privNode, String action, Node segmentNode, Node resourceNode )
    {
        privNode.setProperty( "action", action );
        privNode.createRelationshipTo( segmentNode, SCOPE );
        privNode.createRelationshipTo( resourceNode, APPLIES_TO );
    }

    // INITIALIZATION

    @Override
    public void setUpDefaultPrivileges( Transaction tx )
    {
        // Check for DatabaseAll node to see if the default privileges were already setup
        if ( nodesWithLabelExist( tx, EnterpriseVersion_3_41d1.DATABASE_ALL_LABEL ) )
        {
            return;
        }

        // Create a DatabaseAll node
        Node allDb = tx.createNode( EnterpriseVersion_3_41d1.DATABASE_ALL_LABEL );
        allDb.setProperty( "name", "*" );

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
        traverseNodePriv = tx.createNode( PRIVILEGE_LABEL );
        traverserRelPriv = tx.createNode( PRIVILEGE_LABEL );
        readNodePriv = tx.createNode( PRIVILEGE_LABEL );
        readRelPriv = tx.createNode( PRIVILEGE_LABEL );
        writeNodePriv = tx.createNode( PRIVILEGE_LABEL );
        writeRelPriv = tx.createNode( PRIVILEGE_LABEL );
        accessPriv = tx.createNode( PRIVILEGE_LABEL );
        tokenPriv = tx.createNode( PRIVILEGE_LABEL );
        schemaPriv = tx.createNode( PRIVILEGE_LABEL );
        adminPriv = tx.createNode( PRIVILEGE_LABEL );

        setupPrivilegeNode( traverseNodePriv, PrivilegeAction.TRAVERSE.toString(), labelSegment, graphResource );
        setupPrivilegeNode( traverserRelPriv, PrivilegeAction.TRAVERSE.toString(), relSegment, graphResource );
        setupPrivilegeNode( readNodePriv, PrivilegeAction.READ.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( readRelPriv, PrivilegeAction.READ.toString(), relSegment, allPropResource );
        setupPrivilegeNode( writeNodePriv, PrivilegeAction.WRITE.toString(), labelSegment, allPropResource );
        setupPrivilegeNode( writeRelPriv, PrivilegeAction.WRITE.toString(), relSegment, allPropResource );
        setupPrivilegeNode( accessPriv, PrivilegeAction.ACCESS.toString(), dbSegment, dbResource );
        setupPrivilegeNode( tokenPriv, PrivilegeAction.TOKEN.toString(), dbSegment, dbResource );
        setupPrivilegeNode( schemaPriv, "schema", dbSegment, dbResource );
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
            role.createRelationshipTo( schemaPriv, GRANTED );

        case PUBLISHER:
            role.createRelationshipTo( tokenPriv, GRANTED );

        case EDITOR:
            // The segment part is ignored for this action
            role.createRelationshipTo( writeNodePriv, GRANTED );
            role.createRelationshipTo( writeRelPriv, GRANTED );

        case READER:
            role.createRelationshipTo( traverseNodePriv, GRANTED );
            role.createRelationshipTo( traverserRelPriv, GRANTED );
            role.createRelationshipTo( readNodePriv, GRANTED );
            role.createRelationshipTo( readRelPriv, GRANTED );
            role.createRelationshipTo( accessPriv, GRANTED );

        default:
        }
    }

    // UPGRADE

    @Override
    public void upgradeSecurityGraph( Transaction tx, KnownEnterpriseSecurityComponentVersion latest )
    {
        assert latest.version == 4;
        // Upgrade from 4.0.x to 4.1.0-Drop02, which means add the Version node, change global writes and split schema into index and constraint
        setVersionProperty( tx, latest.version );
        upgradeWriteFromAllPropertiesToGraphResource( tx );
        upgradeFromSchemaPrivilegeToIndexAndContraintPrivileges( tx );
        newRole( tx, PUBLIC );
    }

    private void upgradeWriteFromAllPropertiesToGraphResource( Transaction tx )
    {
        Node graphResource = tx.findNode( Label.label( "Resource" ), "type", Resource.Type.GRAPH.toString() );
        ResourceIterator<Node> writeNodes = tx.findNodes( PRIVILEGE_LABEL, "action", PrivilegeAction.WRITE.toString() );
        while ( writeNodes.hasNext() )
        {
            Node writeNode = writeNodes.next();
            Relationship writeResourceRel = writeNode.getSingleRelationship( APPLIES_TO, Direction.OUTGOING );
            Node oldResource = writeResourceRel.getEndNode();
            if ( !oldResource.getProperty( "type" ).equals( Resource.Type.GRAPH.toString() ) )
            {
                writeNode.createRelationshipTo( graphResource, APPLIES_TO );
                writeResourceRel.delete();
            }
        }
        writeNodes.close();
    }

    private void upgradeFromSchemaPrivilegeToIndexAndContraintPrivileges( Transaction tx )
    {
        // migrate schema privilege to index + constraint privileges
        Node schemaNode = tx.findNode( PRIVILEGE_LABEL, "action", "schema" );
        if ( schemaNode == null )
        {
            return;
        }
        Relationship schemaSegmentRel = schemaNode.getSingleRelationship( SCOPE, Direction.OUTGOING );
        Relationship schemaResourceRel = schemaNode.getSingleRelationship( APPLIES_TO, Direction.OUTGOING );

        Node segment = schemaSegmentRel.getEndNode();
        Node resource = schemaResourceRel.getEndNode();

        Node indexNode = tx.findNode( PRIVILEGE_LABEL, "action", PrivilegeAction.INDEX.toString() );
        if ( indexNode == null )
        {
            indexNode = tx.createNode( PRIVILEGE_LABEL );
            setupPrivilegeNode( indexNode, PrivilegeAction.INDEX.toString(), segment, resource );
        }
        Node constraintNode = tx.findNode( PRIVILEGE_LABEL, "action", PrivilegeAction.CONSTRAINT.toString() );
        if ( constraintNode == null )
        {
            constraintNode = tx.createNode( PRIVILEGE_LABEL );
            setupPrivilegeNode( constraintNode, PrivilegeAction.CONSTRAINT.toString(), segment, resource );
        }

        for ( Relationship rel : schemaNode.getRelationships( GRANTED ) ) // incoming from roles
        {
            Node role = rel.getOtherNode( schemaNode );
            role.createRelationshipTo( indexNode, GRANTED );
            role.createRelationshipTo( constraintNode, GRANTED );
            rel.delete();
        }

        schemaResourceRel.delete();
        schemaSegmentRel.delete();
        schemaNode.delete();
    }

    // RUNTIME

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        if ( SpecialDatabase.DEFAULT == specialDatabase )
        {
            throw unsupportedAction();
        }

        switch ( action )
        {
        case CREATE_USER:
        case DROP_USER:
        case SHOW_USER:
        case SET_USER_STATUS:
        case SET_PASSWORDS:
        case ALTER_USER:
        case USER_MANAGEMENT:

        case CREATE_DATABASE:
        case DROP_DATABASE:
        case DATABASE_MANAGEMENT:

        case SHOW_PRIVILEGE:
        case ASSIGN_PRIVILEGE:
        case REMOVE_PRIVILEGE:
        case PRIVILEGE_MANAGEMENT:

        case DBMS_ACTIONS:

        case TRANSACTION_MANAGEMENT:
        case SHOW_TRANSACTION:
        case TERMINATE_TRANSACTION:

        case CREATE_ELEMENT:
        case DELETE_ELEMENT:
        case SET_LABEL:
        case REMOVE_LABEL:
            throw unsupportedAction();

        default:
        }
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        return super.currentGetPrivilegeForRoles( tx, roleNames, privilegeCache );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder_2_40( privilegeType, action );
    }
}
