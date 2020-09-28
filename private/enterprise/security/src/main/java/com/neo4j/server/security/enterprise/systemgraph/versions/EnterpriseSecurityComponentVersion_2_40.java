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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.Log;
import org.neo4j.server.security.systemgraph.ComponentVersion;
import org.neo4j.util.Preconditions;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ADMIN;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.ARCHITECT;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.EDITOR;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.PUBLISHER;
import static com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.server.security.systemgraph.ComponentVersion.LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;

/**
 * This is the EnterpriseSecurityComponent version for Neo4j 4.0
 */
public class EnterpriseSecurityComponentVersion_2_40 extends SupportedEnterpriseSecurityComponentVersion
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

    public EnterpriseSecurityComponentVersion_2_40( Log log )
    {
        super( ComponentVersion.ENTERPRISE_SECURITY_40, log );
    }

    @Override
    public boolean detected( Transaction tx )
    {
        return nodesWithLabelExist( tx, DATABASE_ALL_LABEL ) &&
               !nodesWithLabelExist( tx, DATABASE_DEFAULT_LABEL ) &&
               componentNotInVersionNode( tx );
    }

    // INITIALIZATION

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
        Preconditions.checkState( latest.version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION,
                format("Latest version should be %s but was %s", LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION, latest.version ));
        log.info( String.format( "Upgrading security model from %s by restructuring privileges", this.description ) );
        // Upgrade from 4.0.x to 4.1.x, which means add the Version node, change global writes,split schema into index and constraint and add the public role
        setVersionProperty( tx, latest.version );
        upgradeWriteFromAllPropertiesToGraphResource( tx );
        upgradeFromSchemaPrivilegeToIndexAndContraintPrivileges( tx );
        Node publicRole = createPublicRoleFromUpgrade( tx );
        grantExecuteProcedurePrivilegeTo( tx, publicRole );
        grantExecuteFunctionPrivilegeTo( tx, publicRole );
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
    boolean supportsUpdateAction( PrivilegeAction action )
    {
        switch ( action )
        {
        // Database privileges
        case ACCESS:
        case START_DATABASE:
        case STOP_DATABASE:

        case CREATE_INDEX:
        case DROP_INDEX:
        case INDEX:
        case CREATE_CONSTRAINT:
        case DROP_CONSTRAINT:
        case CONSTRAINT:

        case CREATE_LABEL:
        case CREATE_RELTYPE:
        case CREATE_PROPERTYKEY:
        case TOKEN:

        case DATABASE_ACTIONS:

        // Role management
        case CREATE_ROLE:
        case DROP_ROLE:
        case ASSIGN_ROLE:
        case REMOVE_ROLE:
        case SHOW_ROLE:
        case ROLE_MANAGEMENT:

        // Graph privileges
        case TRAVERSE:
        case READ:
        case MATCH:
        case WRITE:

            return true;
        default:
            return false;
        }
    }

    @Override
    public void assertUpdateWithAction( PrivilegeAction action, SpecialDatabase specialDatabase ) throws UnsupportedOperationException
    {
        if ( !supportsUpdateAction( action ) || SpecialDatabase.DEFAULT == specialDatabase )
        {
            throw unsupportedAction();
        }
    }

    @Override
    public Set<ResourcePrivilege> getPrivilegeForRoles( Transaction tx, List<String> roleNames, Cache<String,Set<ResourcePrivilege>> privilegeCache )
    {
        return super.currentGetPrivilegeForRoles( tx, roleNames, privilegeCache );
    }

    @Override
    Set<ResourcePrivilege> getTemporaryPrivileges() throws InvalidArgumentsException
    {
        return Collections
                .singleton( new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL ) );
    }

    @Override
    public PrivilegeBuilder makePrivilegeBuilder( ResourcePrivilege.GrantOrDeny privilegeType, String action )
    {
        return new PrivilegeBuilder_2_40( privilegeType, action );
    }
}
