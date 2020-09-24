/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.server.security.enterprise.auth.Resource.AllLabelsResource;
import com.neo4j.server.security.enterprise.auth.Resource.AllPropertiesResource;
import com.neo4j.server.security.enterprise.auth.Resource.DatabaseResource;
import com.neo4j.server.security.enterprise.auth.Resource.GraphResource;
import com.neo4j.server.security.enterprise.auth.Resource.LabelResource;
import com.neo4j.server.security.enterprise.auth.Resource.PropertyResource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DATABASE_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.GRAPH_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MERGE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.REMOVE_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_LABEL;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SET_PROPERTY;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;

@SuppressWarnings( "StatementWithEmptyBody" )
class ResourcePrivilegeTest
{
    @Test
    void shouldConstructValidPrivileges() throws InvalidArgumentsException
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( ACCESS.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( TRAVERSE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                }
                else if ( READ.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( MATCH.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( SET_LABEL.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new AllLabelsResource() );
                    assertOk( privilegeType, action, new LabelResource( "foo" ) );
                }
                else if ( REMOVE_LABEL.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new AllLabelsResource() );
                    assertOk( privilegeType, action, new LabelResource( "foo" ) );
                }
                else if ( SET_PROPERTY.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( WRITE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new GraphResource() );
                }
                else if ( MERGE.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new AllPropertiesResource() );
                    assertOk( privilegeType, action, new PropertyResource( "foo" ) );
                }
                else if ( ADMIN.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( INDEX.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( CONSTRAINT.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( TOKEN.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( EXECUTE.satisfies( action ) || EXECUTE_BOOSTED.satisfies( action ) || EXECUTE_ADMIN.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else if ( GRAPH_ACTIONS.satisfies( action ) )
                {
                    // grouping of other privileges that are already tested
                }
                else if ( DATABASE_ACTIONS.satisfies( action ) )
                {
                    assertOk( privilegeType, action, new DatabaseResource() );
                }
                else
                {
                    fail( "Unhandled PrivilegeAction: " + action );
                }
            }
        }
    }

    @Test
    void shouldNotAcceptInvalidPrivileges()
    {
        for ( ResourcePrivilege.GrantOrDeny privilegeType : ResourcePrivilege.GrantOrDeny.values() )
        {
            for ( PrivilegeAction action : PrivilegeAction.values() )
            {
                if ( ACCESS.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( TRAVERSE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new DatabaseResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( READ.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new DatabaseResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( MATCH.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new DatabaseResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( SET_LABEL.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else if ( REMOVE_LABEL.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new DatabaseResource() );
                }
                else if ( WRITE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new DatabaseResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( MERGE.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new DatabaseResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( ADMIN.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( INDEX.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( CONSTRAINT.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( TOKEN.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( EXECUTE.satisfies( action ) || EXECUTE_BOOSTED.satisfies( action ) || EXECUTE_ADMIN.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else if ( GRAPH_ACTIONS.satisfies( action ) )
                {
                    // grouping of other privileges that are already tested
                }
                else if ( DATABASE_ACTIONS.satisfies( action ) )
                {
                    assertFail( privilegeType, action, new GraphResource() );
                    assertFail( privilegeType, action, new PropertyResource( "foo" ) );
                    assertFail( privilegeType, action, new AllPropertiesResource() );
                    assertFail( privilegeType, action, new LabelResource( "foo" ) );
                    assertFail( privilegeType, action, new AllLabelsResource() );
                }
                else
                {
                    fail( "Unhandled PrivilegeAction: " + action );
                }
            }
        }
    }

    private void assertFail( ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource )
    {
        assertThrows( InvalidArgumentsException.class, () -> new ResourcePrivilege( privilegeType, action, resource, TEST_SEGMENT, SpecialDatabase.ALL ) );
    }

    private void assertOk( ResourcePrivilege.GrantOrDeny privilegeType, PrivilegeAction action, Resource resource ) throws InvalidArgumentsException
    {
        new ResourcePrivilege( privilegeType, action, resource, TEST_SEGMENT, SpecialDatabase.ALL );
    }

    private static final Segment TEST_SEGMENT = segment -> false;
}
