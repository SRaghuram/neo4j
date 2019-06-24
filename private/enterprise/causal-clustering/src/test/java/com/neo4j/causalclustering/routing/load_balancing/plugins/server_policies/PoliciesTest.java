/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class PoliciesTest
{
    private Log log = mock( Log.class );

    @Test
    public void shouldSupplyDefaultUnfilteredPolicyForEmptyContext() throws Exception
    {
        // given
        Policies policies = new Policies( log );

        // when
        Policy policy = policies.selectFor( EMPTY_MAP );
        Set<ServerInfo> input = asSet(
                new ServerInfo( new SocketAddress( "bolt", 1 ), new MemberId( UUID.randomUUID() ), asSet( "groupA" ) ),
                new ServerInfo( new SocketAddress( "bolt", 2 ), new MemberId( UUID.randomUUID() ), asSet( "groupB" ) )
        );

        Set<ServerInfo> output = policy.apply( input );

        // then
        assertEquals( input, output );
        assertEquals( Policies.DEFAULT_POLICY, policy );
    }

    @Test
    public void shouldThrowExceptionOnUnknownPolicyName()
    {
        // given
        Policies policies = new Policies( log );

        try
        {
            // when
            policies.selectFor( stringMapValue( Policies.POLICY_KEY, "unknown-policy" ) );
            fail();
        }
        catch ( ProcedureException e )
        {
            // then
            assertEquals( Status.Procedure.ProcedureCallFailed, e.status() );
        }
    }

    @Test
    public void shouldThrowExceptionOnSelectionOfUnregisteredDefault()
    {
        Policies policies = new Policies( log );

        try
        {
            // when
            policies.selectFor( stringMapValue( Policies.POLICY_KEY, Policies.DEFAULT_POLICY_NAME ) );
            fail();
        }
        catch ( ProcedureException e )
        {
            // then
            assertEquals( Status.Procedure.ProcedureCallFailed, e.status() );
        }
    }

    @Test
    public void shouldAllowOverridingDefaultPolicy() throws Exception
    {
        Policies policies = new Policies( log );

        String defaultPolicyName = Policies.DEFAULT_POLICY_NAME;
        Policy defaultPolicy = new FilteringPolicy( new AnyGroupFilter( "groupA", "groupB" ) );

        // when
        policies.addPolicy( defaultPolicyName, defaultPolicy );
        Policy selectedPolicy = policies.selectFor( EMPTY_MAP );

        // then
        assertEquals( defaultPolicy, selectedPolicy );
        assertNotEquals( Policies.DEFAULT_POLICY, selectedPolicy );
    }

    @Test
    public void shouldAllowLookupOfAddedPolicy() throws Exception
    {
        // given
        Policies policies = new Policies( log );

        String myPolicyName = "china";
        Policy myPolicy = data -> data;

        // when
        policies.addPolicy( myPolicyName, myPolicy );
        Policy selectedPolicy = policies.selectFor( stringMapValue( Policies.POLICY_KEY, myPolicyName ) );

        // then
        assertEquals( myPolicy, selectedPolicy );
    }

    private MapValue stringMapValue( String...keyValues )
    {
        MapValueBuilder builder = new MapValueBuilder();
        for ( int i = 0; i < keyValues.length; i += 2 )
        {
            builder.add( keyValues[i], stringValue( keyValues[i + 1] ) );
        }
        return builder.build();
    }
}
