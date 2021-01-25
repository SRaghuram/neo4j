/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import java.util.function.Predicate;

public final class Jdk extends InfrastructureCapability<String>
{

    public static InfrastructureCapability of( String vendor, String edition )
    {
        return new Jdk( String.format( "%s-%s", vendor, edition ) );
    }

    public static InfrastructureCapability of( String jdk )
    {
        return new Jdk( jdk );
    }

    private Jdk( String jdk )
    {
        super( jdk );
    }

    @Override
    public Predicate<InfrastructureCapability<?>> predicate()
    {
        return caprequestedCapability ->
                caprequestedCapability.getClass() == getClass() &&
                caprequestedCapability.value().equals( value() );
    }
}
