package com.neo4j.bench.micro.data;

import java.util.SplittableRandom;

public interface ValueGeneratorFun<T>
{
    /**
     * @return true if generator has exhausted its value space and started again from the beginning, false otherwise
     */
    boolean wrapped();

    /**
     * @return next value in sequence
     */
    T next( SplittableRandom rng );
}
