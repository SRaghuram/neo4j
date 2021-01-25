/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public
@interface FixedRate
{
    int period() default 5;

    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
