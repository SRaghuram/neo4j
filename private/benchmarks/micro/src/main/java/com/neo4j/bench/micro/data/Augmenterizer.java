/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.data;

import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.micro.data.Stores.StoreAndConfig;

public abstract class Augmenterizer
{
    /**
     * augment (verb): to make (something) greater by adding to it.
     * <p>
     * This method allows a benchmark to perform additional data generation, that augments the already generated store.
     * <p>
     * Method is called after standard store generation, but before the store is cached.
     * Meaning augmentations will be cached along with the original store, and be available to all forks.
     */
    public abstract void augment( int threads, StoreAndConfig storeAndConfig );

    /**
     * Used to calculate equality of {@link com.neo4j.bench.micro.data.Augmenterizer} instances.
     * <p>
     * By default, return value is the fully parameterized name of the benchmark being run.
     *
     * @return key
     */
    public String augmentKey( FullBenchmarkName benchmarkName )
    {
        return benchmarkName.sanitizedName();
    }

    public static class NullAugmenterizer extends Augmenterizer
    {
        public static final String AUGMENT_KEY = "NONE";

        @Override
        public void augment( int threads, StoreAndConfig storeAndConfig )
        {
            // do nothing
        }

        public String augmentKey( FullBenchmarkName benchmarkName )
        {
            return "NONE";
        }
    }
}
