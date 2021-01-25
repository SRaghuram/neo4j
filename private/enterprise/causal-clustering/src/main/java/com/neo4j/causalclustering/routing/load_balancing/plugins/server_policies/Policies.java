/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;

public class Policies
{
    public static final String POLICY_KEY = "policy";
    static final String DEFAULT_POLICY_NAME = "default";
    static final Policy DEFAULT_POLICY = new FilteringPolicy( IdentityFilter.as() ); // the default default

    private final Map<String,Policy> policies = new HashMap<>();

    private final Log log;

    Policies( Log log )
    {
        this.log = log;
    }

    void addPolicy( String policyName, Policy policy )
    {
        Policy oldPolicy = policies.putIfAbsent( policyName, policy );
        if ( oldPolicy != null )
        {
            log.error( format( "Policy name conflict for '%s'.", policyName ) );
        }
    }

    Policy selectFor( MapValue context ) throws ProcedureException
    {
        AnyValue policyName = context.get( POLICY_KEY );

        if ( policyName == Values.NO_VALUE )
        {
            return defaultPolicy();
        }
        else
        {
            Policy selectedPolicy = policies.get( ((TextValue) policyName ).stringValue() );
            if ( selectedPolicy == null )
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                        format( "Policy definition for '%s' could not be found.", policyName ) );
            }
            return selectedPolicy;
        }
    }

    private Policy defaultPolicy()
    {
        Policy registeredDefault = policies.get( DEFAULT_POLICY_NAME );
        return registeredDefault != null ? registeredDefault : DEFAULT_POLICY;
    }
}
