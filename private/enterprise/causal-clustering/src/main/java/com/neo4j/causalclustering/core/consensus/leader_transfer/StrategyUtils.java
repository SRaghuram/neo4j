/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Collection of utility methods useful when writing {@link SelectionStrategy} implementations
 */
final class StrategyUtils
{
    private StrategyUtils()
    {
    }

    static <T> Optional<T> selectRandom( final Collection<T> elements )
    {
        if ( elements.isEmpty() )
        {
            return Optional.empty();
        }

        var randomElementIdx = ThreadLocalRandom.current().nextInt( elements.size() );
        return elements.stream().skip( randomElementIdx ).findFirst();
    }
}
