/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.resources;

import java.util.function.Predicate;

public final class Hardware
{
    public static AvailableCores availableCores( String availableCores )
    {
        return new AvailableCores( Integer.parseInt( availableCores ) );
    }

    public static AvailableCores availableCores( int availableCores )
    {
        return new AvailableCores( availableCores );
    }

    public static TotalMemory totalMemory( String totalMemory )
    {
        return new TotalMemory( Long.parseLong( totalMemory ) );
    }

    public static TotalMemory totalMemory( long totalMemory )
    {
        return new TotalMemory( totalMemory );
    }

    public static class AvailableCores extends InfrastructureCapability<Integer>
    {

        private AvailableCores( Integer availableCores )
        {
            super( availableCores );
        }

        @Override
        public Predicate<InfrastructureCapability<?>> predicate()
        {
            return requestedCapability ->
                    requestedCapability.getClass() == getClass() &&
                    requestedCapability.value().equals( value() );
        }
    }

    public static class TotalMemory extends InfrastructureCapability<Long>
    {
        private TotalMemory( long totalMemory )
        {
            super( totalMemory );
        }

        /**
         * AWS batch uses ECS underneath, it has a simple docker container allocation strategy, fit as much as you can into a single EC2 instance,
         * as long as we have enough resources (CPU and memory). Because we want to run a single container on EC2 instance container capabilities
         * must match EC2 instance capabilities, it simple for CPUs, EC2 available core must be equal container vcpus. For memory, it is not that simple though.
         * Because we have docker/ECS agent running on EC2 instance, we have slightly less memory available for container than EC2 total memory.
         * We need to make sure that we select instance type that will fit only a single docker container, given the provided amount of memory.
         * I hope it helped explain the logic behind math in this method  ;)
         *
         * @return infrastructure capability predicate
         */
        @Override
        public Predicate<InfrastructureCapability<?>> predicate()
        {
            return requestedCapability ->
                    requestedCapability.getClass() == getClass() &&
                    // hardware has at least as much memory as is required
                    ((Long) requestedCapability.value()) <= value() &&
                    // no more than one docker container canfit on an instance (do not want to share instances)
                    value() / ((Long) requestedCapability.value() * 2) == 0;
        }
    }
}
