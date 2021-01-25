/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.eclipse.collections.api.set.primitive.IntSet;

class FunctionPrivileges
{
    private final boolean allowExecuteAllFunctions;
    private final boolean disallowExecuteAllFunctions;
    private final IntSet allowExecuteFunctions;
    private final IntSet allowExecuteAggregatingFunctions;
    private final IntSet disallowExecuteFunctions;
    private final IntSet disallowExecuteAggregatingFunctions;
    private final boolean allowExecuteBoostedAllFunctions;
    private final boolean disallowExecuteBoostedAllFunctions;
    private final IntSet allowExecuteBoostedFunctions;
    private final IntSet allowExecuteBoostedAggregatingFunctions;
    private final IntSet disallowExecuteBoostedFunctions;
    private final IntSet disallowExecuteBoostedAggregatingFunctions;

    FunctionPrivileges( boolean allowExecuteAllFunctions,
                        boolean disallowExecuteAllFunctions,
                        IntSet allowExecuteFunctions,
                        IntSet allowExecuteAggregatingFunctions,
                        IntSet disallowExecuteFunctions,
                        IntSet disallowExecuteAggregatingFunctions,
                        boolean allowExecuteBoostedAllFunctions,
                        boolean disallowExecuteBoostedAllFunctions,
                        IntSet allowExecuteBoostedFunctions,
                        IntSet allowExecuteBoostedAggregatingFunctions,
                        IntSet disallowExecuteBoostedFunctions,
                        IntSet disallowExecuteBoostedAggregatingFunctions )
    {
        this.allowExecuteAllFunctions = allowExecuteAllFunctions;
        this.disallowExecuteAllFunctions = disallowExecuteAllFunctions;
        this.allowExecuteFunctions = allowExecuteFunctions;
        this.allowExecuteAggregatingFunctions = allowExecuteAggregatingFunctions;
        this.disallowExecuteFunctions = disallowExecuteFunctions;
        this.disallowExecuteAggregatingFunctions = disallowExecuteAggregatingFunctions;
        this.allowExecuteBoostedAllFunctions = allowExecuteBoostedAllFunctions;
        this.disallowExecuteBoostedAllFunctions = disallowExecuteBoostedAllFunctions;
        this.allowExecuteBoostedFunctions = allowExecuteBoostedFunctions;
        this.allowExecuteBoostedAggregatingFunctions = allowExecuteBoostedAggregatingFunctions;
        this.disallowExecuteBoostedFunctions = disallowExecuteBoostedFunctions;
        this.disallowExecuteBoostedAggregatingFunctions = disallowExecuteBoostedAggregatingFunctions;
    }

    boolean allowExecuteFunction( int id )
    {
        if ( disallowExecuteAllFunctions || disallowExecuteFunctions.contains( id ) )
        {
            return false;
        }
        return allowExecuteAllFunctions || allowExecuteFunctions.contains( id ) || shouldBoostFunction( id );
    }

    boolean shouldBoostFunction( int id )
    {
        if ( disallowExecuteBoostedAllFunctions || disallowExecuteBoostedFunctions.contains( id ) )
        {
            return false;
        }
        return allowExecuteBoostedAllFunctions || allowExecuteBoostedFunctions.contains( id );
    }

    boolean allowExecuteAggregatingFunction( int id )
    {
        if ( disallowExecuteAllFunctions || disallowExecuteAggregatingFunctions.contains( id ) )
        {
            return false;
        }
        return allowExecuteAllFunctions || allowExecuteAggregatingFunctions.contains( id ) || shouldBoostAggregatingFunction( id );
    }

    boolean shouldBoostAggregatingFunction( int id )
    {
        if ( disallowExecuteBoostedAllFunctions || disallowExecuteBoostedAggregatingFunctions.contains( id ) )
        {
            return false;
        }
        return allowExecuteBoostedAllFunctions || allowExecuteBoostedAggregatingFunctions.contains( id );
    }
}
