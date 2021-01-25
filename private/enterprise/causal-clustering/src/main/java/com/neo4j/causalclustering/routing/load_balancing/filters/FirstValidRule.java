/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Each chain of filters is considered a rule and they are evaluated in order. The result
 * of the first rule to return a valid result (non-empty set) will be the final result.
 */
public class FirstValidRule<T> implements Filter<T>
{
    private List<FilterChain<T>> rules;

    public FirstValidRule( List<FilterChain<T>> rules )
    {
        this.rules = rules;
    }

    @Override
    public Set<T> apply( Set<T> input )
    {
        Set<T> output = input;
        for ( Filter<T> chain : rules )
        {
            output = chain.apply( input );
            if ( !output.isEmpty() )
            {
                break;
            }
        }
        return output;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FirstValidRule<?> that = (FirstValidRule<?>) o;
        return Objects.equals( rules, that.rules );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( rules );
    }

    @Override
    public String toString()
    {
        return "FirstValidRule{" +
               "rules=" + rules +
               '}';
    }
}
