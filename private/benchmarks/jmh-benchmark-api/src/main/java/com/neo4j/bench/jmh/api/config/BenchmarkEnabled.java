/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
/**
 * This annotation sets the default enable/disable value of benchmarks (classes that extend AbstractBenchmark).
 *   * enabled: by default benchmark will be executed (but can be disabled via configuration).
 *   * disabled: by default benchmark will not be executed (but can be enabled via configuration).
 *
 * Annotation is not compulsory, if omitted it defaults to true/enabled.
 */
public @interface BenchmarkEnabled
{
    boolean value();
}
