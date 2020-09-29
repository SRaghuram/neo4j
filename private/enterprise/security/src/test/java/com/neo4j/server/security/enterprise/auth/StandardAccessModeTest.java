/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.impl.newapi.Labels;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.DENY;
import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CREATE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DELETE_ELEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MERGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

@SuppressWarnings( "Convert2MethodRef" )
class StandardAccessModeTest
{
    private static final long A = 0;
    private static final long B = 1;
    private static final int R1 = 2;
    private static final int R2 = 3;
    private static final int PROP1 = 4;
    private static final int PROP2 = 5;

    private static final int PROC1 = 1;
    private static final int PROC2 = 2;
    private static final int PROC3 = 3;
    private static final int PROC4 = 4;

    private StandardAccessModeBuilder builder;

    @BeforeEach
    void setup()
    {
        LoginContext.IdLookup lookup = mock( LoginContext.IdLookup.class );
        when( lookup.getLabelId( "A" ) ).thenReturn( (int) A );
        when( lookup.getLabelId( "B" ) ).thenReturn( (int) B );
        when( lookup.getRelTypeId( "R1" ) ).thenReturn( R1 );
        when( lookup.getRelTypeId( "R2" ) ).thenReturn( R2 );
        when( lookup.getPropertyKeyId( "PROP1" ) ).thenReturn( PROP1 );
        when( lookup.getPropertyKeyId( "PROP2" ) ).thenReturn( PROP2 );

        when( lookup.getProcedureIds( "apoc.proc1" ) ).thenReturn( new int[]{PROC1} );
        when( lookup.getProcedureIds( "apoc.proc2" ) ).thenReturn( new int[]{PROC2} );
        when( lookup.getProcedureIds( "math.proc3" ) ).thenReturn( new int[]{PROC3} );
        when( lookup.getProcedureIds( "math.proc4" ) ).thenReturn( new int[]{PROC4} );
        when( lookup.getProcedureIds( "*" ) ).thenReturn( new int[]{PROC1, PROC2, PROC3, PROC4} );
        when( lookup.getProcedureIds( "*math*" ) ).thenReturn( new int[]{PROC3, PROC4} );
        when( lookup.getAdminProcedureIds() ).thenReturn( new int[]{PROC1, PROC3} );

        builder = new StandardAccessModeBuilder( true, false, Collections.emptySet(), lookup, DEFAULT_DATABASE_NAME, DEFAULT_DATABASE_NAME );
    }

    @Test
    void shouldAllowNothingByDefault()
    {
        StandardAccessMode mode = builder.build();

        assertThat( mode.allowsAccess() ).isEqualTo( false );

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );

        // WRITE
        assertThat( mode.allowsWrites() ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( false );

        // DELETE
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( false );

        // SET/REMOVE LABEL
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( false );

        // SET PROPERTY
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );

        // NAME MANAGEMENT
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_LABEL ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_RELTYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_PROPERTYKEY ) ).isEqualTo( false );

        // INDEX/CONSTRAINT MANAGEMENT
        assertThat( mode.allowsSchemaWrites() ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isEqualTo( false );

        // EXECUTE PROCEDURES
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    // TRAVERSE NODES

    @Test
    void shouldAllowTraverseLabel() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowTraverseAllNodes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseNode() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // DENY TRAVERSE ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, TRAVERSE, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    // TRAVERSE RELATIONSHIPS

    @Test
    void shouldAllowTraverseAllRelTypes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( true );
    }

    @Test
    void shouldAllowTraverseRelType() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseRelType() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // DENY TRAVERSE ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, TRAVERSE, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    // READ {PROP} ON NODES

    @Test
    void shouldAllowReadAllPropertiesAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllNodes() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    // READ {PROP} ON RELATIONSHIPS

    @Test
    void shouldAllowReadAllPropertiesAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllRelationships() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // GRANT READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {*} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    // SEE PROP IN TOKEN STORE

    @Test
    void shouldAllowSeeAllPropertyTokensNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeeAllPropertyTokensRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenNode() throws Exception
    {
        // WHEN
        // GRANT READ {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSeePropertyTokenRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {PROP2} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, READ, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {PROP2} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP2" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificNode() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES A
        // DENY READ {PROP1} ON GRAPH neo4j NODES B
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificRelationship() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS R1
        // DENY READ {PROP1} ON GRAPH neo4j RELATIONSHIPS R2
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R2" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantNodesDenyRelationships() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j NODES *
        // DENY READ {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantRelationshipsDenyNodes() throws Exception
    {
        // WHEN
        // GRANT READ {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY READ {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    // MATCH: TRAVERSE NODES

    @Test
    void shouldAllowTraverseLabelWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowTraverseAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( B ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A, B ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( B ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );
    }

    // MATCH: TRAVERSE RELATIONSHIPS

    @Test
    void shouldAllowTraverseAllRelTypesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( true );
    }

    @Test
    void shouldAllowTraverseRelTypeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    @Test
    void shouldDenyTraverseRelTypeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( R2 ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
    }

    // MATCH {PROP} ON NODES

    @Test
    void shouldAllowReadAllPropertiesAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j NODES *
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( false );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( Labels::from, PROP2 ) ).isEqualTo( true );
    }

    // MATCH {PROP} ON RELATIONSHIPS

    @Test
    void shouldAllowReadAllPropertiesAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowReadPropertyAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertySomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowReadPropertyTraverseAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT TRAVERSE ON GRAPH neo4j RELATIONSHIPS *
        // GRANT MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege1 = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyReadPropertyAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadPropertySomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyReadAllPropertiesSomeRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS A
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    // MATCH: SEE PROP IN TOKEN STORE

    @Test
    void shouldAllowSeeAllPropertyTokensNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeeAllPropertyTokensRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP1} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSeePropertyTokenRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {PROP2} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, MATCH, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenAllRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {PROP2} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP2" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificNodeWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES A
        // DENY MATCH {PROP1} ON GRAPH neo4j NODES B
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySeePropertyTokenSpecificRelationshipWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS R1
        // DENY MATCH {PROP1} ON GRAPH neo4j RELATIONSHIPS R2
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R2" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantNodesDenyRelationshipsWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j NODES *
        // DENY MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSeePropertyTokenGrantRelationshipsDenyNodesWithMatchPrivilege() throws Exception
    {
        // WHEN
        // GRANT MATCH {*} ON GRAPH neo4j RELATIONSHIPS *
        // DENY MATCH {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isEqualTo( true );
    }

    // WRITE

    @Test
    void shouldAllowWrites() throws Exception
    {
        // WHEN
        // GRANT WRITE ON GRAPH neo4j
        var privilege1 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsWrites() ).isEqualTo( true );
    }

    // CREATE

    @Test
    void shouldAllowCreateNodes() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsCreateNode( null ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowCreateSpecificNode() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsCreateNode( null ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyCreateNode() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j NODES *
        // DENY CREATE ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, CREATE_ELEMENT, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsCreateNode( null ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowCreateRelationships() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j RELATIONSHIP *
        var privilege = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsCreateRelationship( R2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowCreateSpecificRelationships() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j RELATIONSHIP R1
        var privilege = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsCreateRelationship( R2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyCreateRelationships() throws Exception
    {
        // WHEN
        // GRANT CREATE ON GRAPH neo4j RELATIONSHIP *
        // DENY CREATE ON GRAPH neo4j RELATIONSHIP R1
        var privilege1 = new ResourcePrivilege( GRANT, CREATE_ELEMENT, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, CREATE_ELEMENT, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsCreateRelationship( R2 ) ).isEqualTo( true );
    }

    // DELETE

    @Test
    void shouldAllowDeleteNodes() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsDeleteNode( () -> Labels.NONE ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( B ) ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A, B ) ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowDeleteSpecificNode() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsDeleteNode( () -> Labels.NONE ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( B ) ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A, B ) ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyDeleteNode() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j NODES *
        // DENY DELETE ON GRAPH neo4j NODES A
        var privilege1 = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, DELETE_ELEMENT, new Resource.GraphResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsDeleteNode( () -> Labels.NONE ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( B ) ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A, B ) ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowDeleteRelationships() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j RELATIONSHIP *
        var privilege = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteRelationship( R2 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowDeleteSpecificRelationships() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j RELATIONSHIP R1
        var privilege = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteRelationship( R2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyDeleteRelationships() throws Exception
    {
        // WHEN
        // GRANT DELETE ON GRAPH neo4j RELATIONSHIP *
        // DENY DELETE ON GRAPH neo4j RELATIONSHIP R1
        var privilege1 = new ResourcePrivilege( GRANT, DELETE_ELEMENT, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, DELETE_ELEMENT, new Resource.GraphResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteRelationship( R2 ) ).isEqualTo( true );
    }

    // SET LABEL

    @Test
    void shouldAllowSetLabel() throws Exception
    {
        // GRANT SET LABEL * ON GRAPH neo4j
        var privilege = new ResourcePrivilege( GRANT, SET_LABEL, new Resource.AllLabelsResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsSetLabel( B ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSetSpecificLabel() throws Exception
    {
        // GRANT SET LABEL A ON GRAPH neo4j
        var privilege = new ResourcePrivilege( GRANT, SET_LABEL, new Resource.LabelResource( "A" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsSetLabel( B ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySetLabelSpecificLabel() throws Exception
    {
        // GRANT SET LABEL * ON GRAPH neo4j
        // DENY SET LABEL A ON GRAPH neo4j
        var privilege1 = new ResourcePrivilege( GRANT, SET_LABEL, new Resource.AllLabelsResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_LABEL, new Resource.LabelResource( "A" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsSetLabel( B ) ).isEqualTo( true );
    }

    // REMOVE LABEL

    @Test
    void shouldAllowRemoveLabel() throws Exception
    {
        // GRANT REMOVE LABEL * ON GRAPH neo4j
        var privilege = new ResourcePrivilege( GRANT, REMOVE_LABEL, new Resource.AllLabelsResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsRemoveLabel( B ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowRemoveSpecificLabel() throws Exception
    {
        // GRANT REMOVE LABEL A ON GRAPH neo4j
        var privilege = new ResourcePrivilege( GRANT, REMOVE_LABEL, new Resource.LabelResource( "A" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // THEN
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsRemoveLabel( B ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyRemoveLabelSpecificLabel() throws Exception
    {
        // GRANT REMOVE LABEL * ON GRAPH neo4j
        // DENY REMOVE LABEL A ON GRAPH neo4j
        var privilege1 = new ResourcePrivilege( GRANT, REMOVE_LABEL, new Resource.AllLabelsResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, REMOVE_LABEL, new Resource.LabelResource( "A" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // THEN
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsRemoveLabel( B ) ).isEqualTo( true );
    }

    // SET PROPERTY

    @Test
    void shouldAllowSetPropertyAllNodes() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSetSpecificPropertyAllNodes() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j NODES *
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSetPropertySpecificLabel() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSetSpecificPropertySpecificLabel() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j NODES A
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySetPropertyOnNodeBeforeGrant() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySetPropertyOnNode() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j NODES *
        // DENY SET PROPERTY { PROP1 } ON GRAPH neo4j NODES B
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyAllSetPropertyOverGrantAll() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j NODES *
        // DENY SET PROPERTY {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyAllSetPropertyOverSpecificGrant() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j NODES B
        // DENY SET PROPERTY {*} ON GRAPH neo4j NODES *
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldOverrideWriteWithDenySetPropertyOnASpecificNodeType() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "B" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );
    }

    @Test
    void denyWriteShouldOverrideSetProperty() throws Exception
    {
        var privilege2 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege1 = new ResourcePrivilege( DENY, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSetPropertyAllRelationships() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
    }

    @Test
    void shouldAllowSetSpecificPropertyAllRelationships() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j RELATIONSHIPS *
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSetPropertySpecificRelationships() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIPS R1
        var privilege = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldAllowSetSpecificPropertySpecificRelationships() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j RELATIONSHIPS R1
        var privilege =
                new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenySetPropertyOnRelationshipBeforeGrant() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIP *
        // DENY SET PROPERTY { PROP1 } on GRAPH neo4j RELATIONSHIP *
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenySetPropertyOnRelationship() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIP *
        // DENY SET PROPERTY { PROP1 } ON GRAPH neo4j RELATIONSHIP R1
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void shouldDenyAllSetPropertyOnRelationshipOverGrantAll() throws Exception
    {
        // GRANT SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIP *
        // DENY SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIP *
        var privilege1 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldDenyAllSetPropertyOnRelationshipOverSpecificGrant() throws Exception
    {
        // GRANT SET PROPERTY { PROP1 } ON GRAPH neo4j RELATIONSHIP R1
        // DENY SET PROPERTY {*} ON GRAPH neo4j RELATIONSHIP *
        var privilege1 =
                new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void shouldOverrideWriteWithDenySetPropertyOnASpecificRelationshipType() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( true );
    }

    @Test
    void denyWriteShouldOverrideSetPropertyonRelationship() throws Exception
    {
        var privilege2 = new ResourcePrivilege( GRANT, SET_PROPERTY, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege1 = new ResourcePrivilege( DENY, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    // ALL GRAPH PRIVILEGES

    @Test
    void shouldAllowAllReadsAndWritesWhenGrantedGraphActions() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, GRAPH_ACTIONS, new Resource.GraphResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

         assertThat( mode.allowsAccess() ).isEqualTo( false );

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( true );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( false );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( true );

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( true );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( true );

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( true );

        // WRITE
        assertThat( mode.allowsWrites() ).isEqualTo( true );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( true );

        // DELETE
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( true );
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( true );

        // SET/REMOVE LABEL
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( true );

        // SET PROPERTY
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );

        // NAME MANAGEMENT
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_LABEL ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_RELTYPE ) ).isEqualTo( false );
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_PROPERTYKEY ) ).isEqualTo( false );

        // INDEX/CONSTRAINT MANAGEMENT
        assertThat( mode.allowsSchemaWrites() ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isEqualTo( false );
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isEqualTo( false );
    }

    @Test
    void shouldDisallowAllReadsAndWritesWhenDeniedGraphActions() throws Exception
    {
        var privilege1 = new ResourcePrivilege( DENY, GRAPH_ACTIONS, new Resource.GraphResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( GRANT, MATCH,  new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege3 = new ResourcePrivilege( GRANT, MATCH,  new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege4 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege5 = new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).addPrivilege( privilege3 ).addPrivilege( privilege4 )
                .addPrivilege( privilege5 ).build();

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels() ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseNode( A ) ).isEqualTo( false );

        assertThat( mode.disallowsTraverseLabel( A ) ).isEqualTo( true );
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isEqualTo( true );

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isEqualTo( false );

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes() ).isEqualTo( false );
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isEqualTo( false );

        assertThat( mode.allowsTraverseRelType( R1 ) ).isEqualTo( false );

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isEqualTo( false );

        // WRITE
        assertThat( mode.allowsWrites() ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateRelationship( R1 ) ).isEqualTo( false );

        // DELETE
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isEqualTo( false );
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isEqualTo( false );

        // SET/REMOVE LABEL
        assertThat( mode.allowsSetLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsRemoveLabel( A ) ).isEqualTo( false );

        // SET PROPERTY
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
    }

    @Test
    void shouldDisallowCorrectReadsWhenGrantedGraphActionsButDeniedSpecificReads() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, GRAPH_ACTIONS, new Resource.GraphResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, READ, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isEqualTo( true );

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isEqualTo( true );
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( true );
    }

    // MERGE
    @Test
    void grantMergeOnNodeShouldAllowReadTraverseCreateAndSetProp() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, MERGE, new Resource.PropertyResource( "PROP1" ), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // READ
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );

        // TRAVERSE
        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );

        // SET PROP
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void grantMergeOnRelShouldAllowReadTraverseCreateAndSetProp() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, MERGE, new Resource.PropertyResource( "PROP1" ), new RelTypeSegment( "R1" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege ).build();

        // READ
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( false );

        // TRAVERSE
        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( false );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );

        // SET PROP
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );

        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    @Test
    void grantMergeAndDenyCreateShouldAllowOnlySettingProperty() throws Exception
    {
        var privilege2 = new ResourcePrivilege( GRANT, MERGE, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege1 = new ResourcePrivilege( DENY, CREATE_ELEMENT, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // READ
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        // TRAVERSE
        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );

        // SET PROP
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void grantMergeAndDenySetPropShouldAllowOnlyCreatingNode() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, MERGE, new Resource.AllPropertiesResource(), new LabelSegment( "A" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP2" ), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // READ
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from(), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A, B ), PROP2 ) ).isEqualTo( true );

        // TRAVERSE
        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isEqualTo( true );
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isEqualTo( false );

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isEqualTo( true );
        assertThat( mode.allowsCreateNode( new int[]{(int) B} ) ).isEqualTo( false );
        assertThat( mode.allowsCreateNode( new int[]{(int) A, (int) B} ) ).isEqualTo( false );

        // SET PROP
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> Labels.from( A ), PROP2 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP1 ) ).isEqualTo( false );
        assertThat( mode.allowsSetProperty( () -> Labels.from( B ), PROP2 ) ).isEqualTo( false );
    }

    @Test
    void grantMergeOnALlRelationshipsAndDenySetPropR1ShouldAllowOnlySetPropR2() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, MERGE, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, SET_PROPERTY, new Resource.PropertyResource( "PROP2" ), new RelTypeSegment( "R2" ), DEFAULT_DATABASE_NAME );
        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // READ
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsReadRelationshipProperty( () -> R2, PROP2 ) ).isEqualTo( true );

        // SET PROP
        assertThat( mode.allowsSetProperty( () -> R1, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R1, PROP2 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP1 ) ).isEqualTo( true );
        assertThat( mode.allowsSetProperty( () -> R2, PROP2 ) ).isEqualTo( false );
    }

    // ALL ON DBMS

    @Test
    void grantAllOnDbms() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, DBMS_ACTIONS, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        assertThat( mode.allowsAccess() ).isFalse();

        // TRAVERSE NODES
        assertThat( mode.allowsTraverseAllLabels() ).isFalse();
        assertThat( mode.allowsTraverseNode( ANY_LABEL ) ).isFalse();

        assertThat( mode.allowsTraverseNode( A ) ).isFalse();
        assertThat( mode.allowsTraverseNode( B ) ).isFalse();
        assertThat( mode.allowsTraverseNode( A, B ) ).isFalse();

        assertThat( mode.disallowsTraverseLabel( A ) ).isFalse();
        assertThat( mode.disallowsTraverseLabel( B ) ).isFalse();
        assertThat( mode.disallowsTraverseLabel( ANY_LABEL ) ).isFalse();

        assertThat( mode.allowsTraverseAllNodesWithLabel( A ) ).isFalse();
        assertThat( mode.allowsTraverseAllNodesWithLabel( B ) ).isFalse();
        assertThat( mode.allowsTraverseAllNodesWithLabel( ANY_LABEL ) ).isFalse();

        // TRAVERSE RELATIONSHIPS
        assertThat( mode.allowsTraverseAllRelTypes() ).isFalse();
        assertThat( mode.allowsTraverseRelType( ANY_RELATIONSHIP_TYPE ) ).isFalse();

        assertThat( mode.allowsTraverseRelType( R1 ) ).isFalse();
        assertThat( mode.allowsTraverseRelType( R2 ) ).isFalse();

        // READ {PROP} ON NODES
        assertThat( mode.allowsReadPropertyAllLabels( PROP1 ) ).isFalse();
        assertThat( mode.allowsReadPropertyAllLabels( PROP2 ) ).isFalse();

        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP1 ) ).isFalse();
        assertThat( mode.disallowsReadPropertyForSomeLabel( PROP2 ) ).isFalse();

        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP1 ) ).isFalse();
        assertThat( mode.allowsReadNodeProperty( () -> Labels.from( A ), PROP2 ) ).isFalse();

        // READ {PROP} ON RELATIONSHIPS
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP1 ) ).isFalse();
        assertThat( mode.allowsReadPropertyAllRelTypes( PROP2 ) ).isFalse();

        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP1 ) ).isFalse();
        assertThat( mode.allowsReadRelationshipProperty( () -> R1, PROP2 ) ).isFalse();

        // SEE PROP IN TOKEN STORE
        assertThat( mode.allowsSeePropertyKeyToken( PROP1 ) ).isFalse();
        assertThat( mode.allowsSeePropertyKeyToken( PROP2 ) ).isFalse();

        // WRITE
        assertThat( mode.allowsWrites() ).isFalse();

        // CREATE
        assertThat( mode.allowsCreateNode( new int[]{(int) A} ) ).isFalse();
        assertThat( mode.allowsCreateRelationship( R1 ) ).isFalse();

        // DELETE
        assertThat( mode.allowsDeleteNode( () -> Labels.from( A ) ) ).isFalse();
        assertThat( mode.allowsDeleteRelationship( R1 ) ).isFalse();

        // SET/REMOVE LABEL
        assertThat( mode.allowsSetLabel( A ) ).isFalse();
        assertThat( mode.allowsRemoveLabel( A ) ).isFalse();

        // SET PROPERTY
        assertThat( mode.allowsSetProperty( () -> Labels.from( A, B ), PROP1 ) ).isFalse();

        // NAME MANAGEMENT
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_LABEL ) ).isFalse();
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_RELTYPE ) ).isFalse();
        assertThat( mode.allowsTokenCreates( PrivilegeAction.CREATE_PROPERTYKEY ) ).isFalse();

        // INDEX/CONSTRAINT MANAGEMENT
        assertThat( mode.allowsSchemaWrites() ).isFalse();
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_INDEX ) ).isFalse();
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_INDEX ) ).isFalse();
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.CREATE_CONSTRAINT ) ).isFalse();
        assertThat( mode.allowsSchemaWrites( PrivilegeAction.DROP_CONSTRAINT ) ).isFalse();

        // EXECUTE PROCEDURES
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isTrue();
    }

    // EXECUTE PROCEDURE

    @Test
    void grantExecuteOnAllProcedures() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteOnNamedProcedure() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "apoc.proc2" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteOnGlobbedProcedures() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "*math*" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void denyExecuteOnNamedProcedure() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "*" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "apoc.proc1" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteBoostedOnAllProcedures() throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new Resource.DatabaseResource(), ProcedureSegment.ALL, DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isTrue();
    }

    @Test
    void grantExecuteBoostedOnNamedProcedure() throws Exception
    {
        var privilege =
                new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc4" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isTrue();
    }

    @Test
    void denyExecuteBoostedOnNamedProcedure() throws Exception
    {
        var privilege1 =
                new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "*math*" ), DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc4" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantBoostedOnAllProceduresAndDenyExecuteNamedProcedure() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "*" ), DEFAULT_DATABASE_NAME );
        var privilege2 = new ResourcePrivilege( DENY, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc4" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isTrue();
    }

    @Test
    void grantExecuteOnAllProceduresAndDenyBoostedNamedProcedure() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc4" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantAllOnDbmsAndDenyBoostedNamedProcedure() throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, DBMS_ACTIONS, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc4" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteAdminProcedure()  throws Exception
    {
        var privilege = new ResourcePrivilege( GRANT, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteAdminAndDenyBoostedProcedure()  throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc3" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteAdminAndDenyBoostedAndGrantExecuteProcedure()  throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc3" ), DEFAULT_DATABASE_NAME );
        var privilege3 =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc3" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).addPrivilege( privilege3 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteAdminAndDenyExecuteProcedure()  throws Exception
    {
        var privilege1 = new ResourcePrivilege( GRANT, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( DENY, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "math.proc3" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isFalse();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isTrue();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }

    @Test
    void grantExecuteBoostedAndDenyAdminProcedure()  throws Exception
    {
        var privilege1 = new ResourcePrivilege( DENY, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( GRANT, EXECUTE_BOOSTED, new Resource.DatabaseResource(), new ProcedureSegment( "*math*" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isTrue();
    }

    @Test
    void grantExecuteAndDenyAdminProcedure()  throws Exception
    {
        var privilege1 = new ResourcePrivilege( DENY, EXECUTE_ADMIN, new Resource.DatabaseResource(), Segment.ALL, DEFAULT_DATABASE_NAME );
        var privilege2 =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), new ProcedureSegment( "*math*" ), DEFAULT_DATABASE_NAME );

        var mode = builder.addPrivilege( privilege1 ).addPrivilege( privilege2 ).build();

        // EXECUTE
        assertThat( mode.allowsExecuteProcedure( PROC1 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC2 ) ).isFalse();
        assertThat( mode.allowsExecuteProcedure( PROC3 ) ).isTrue();
        assertThat( mode.allowsExecuteProcedure( PROC4 ) ).isTrue();

        // BOOST
        assertThat( mode.shouldBoostProcedure( PROC1 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC2 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC3 ) ).isFalse();
        assertThat( mode.shouldBoostProcedure( PROC4 ) ).isFalse();
    }
 }
