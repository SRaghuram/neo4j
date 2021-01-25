/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import java.util.function.Predicate;

public final class AWS
{

    public static InfrastructureCapability instanceType( String instanceType )
    {
        return new InstanceType( instanceType );
    }

    static class InstanceType extends InfrastructureCapability
    {

        private InstanceType( String instanceType )
        {
            super( instanceType );
        }

        @Override
        public Predicate<InfrastructureCapability<?>> predicate()
        {
            return requestedCapability ->
                    requestedCapability.getClass() == getClass() &&
                    requestedCapability.value().equals( value() );
        }
    }
}
