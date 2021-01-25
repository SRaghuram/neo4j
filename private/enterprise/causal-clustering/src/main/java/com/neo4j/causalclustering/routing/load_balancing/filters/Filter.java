/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.Set;

/**
 * A filter for sets.
 *
 * A convention used for filters is to return an empty set if the result is to
 * be interpreted as invalid. This is used for example in rule-lists where the first
 * rule to return a valid non-empty result will be used.
 */
@FunctionalInterface
public interface Filter<T>
{
    Set<T> apply( Set<T> data );
}
