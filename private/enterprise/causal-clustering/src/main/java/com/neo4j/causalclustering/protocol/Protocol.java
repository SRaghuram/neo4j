/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Protocol<IMPL extends Comparable<IMPL>>
{
    String category();

    IMPL implementation();

    static <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> Optional<T> find(
            T[] values, Category<T> category, IMPL implementation, Function<IMPL,IMPL> normalise )
    {
        return Stream.of( values )
                .filter( protocol -> Objects.equals( protocol.category(), category.canonicalName() ) )
                .filter( protocol -> Objects.equals( normalise.apply( protocol.implementation() ), normalise.apply( implementation ) ) )
                .findFirst();
    }

    static <IMPL extends Comparable<IMPL>, T extends Protocol<IMPL>> List<T> filterCategory(
            T[] values, Category<T> category, Predicate<IMPL> implPredicate )
    {
        return Stream.of( values )
                .filter( protocol -> Objects.equals( protocol.category(), category.canonicalName() ) )
                .filter( protocol -> implPredicate.test( protocol.implementation() ) )
                .collect( Collectors.toList() );
    }

    interface Category<T extends Protocol<?>>
    {
        String canonicalName();
    }
}
