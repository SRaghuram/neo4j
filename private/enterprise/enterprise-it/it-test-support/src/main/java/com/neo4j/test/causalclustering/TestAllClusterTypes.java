/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.causalclustering;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.util.concurrent.TimeUnit.MINUTES;

@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
@ParameterizedTest( name = "{0}" )
@EnumSource( ClusterConfig.ClusterType.class )
@Timeout( value = 3, unit = MINUTES )
public @interface TestAllClusterTypes
{
}
