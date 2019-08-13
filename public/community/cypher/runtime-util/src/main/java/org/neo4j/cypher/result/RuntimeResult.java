/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.result;

import java.util.Optional;

import org.neo4j.cypher.internal.runtime.QueryStatistics;
import org.neo4j.kernel.impl.query.QuerySubscription;

/**
 * The result API of a Cypher runtime
 */
public interface RuntimeResult extends AutoCloseable, QuerySubscription
{
    enum ConsumptionState
    {
        NOT_STARTED,
        HAS_MORE,
        EXHAUSTED
    };

    /**
     * Names of the returned fields of this result.
     */
    String[] fieldNames();

    /**
     * Returns the consumption state of this result. This state changes when the result is served
     */
    ConsumptionState consumptionState();

    /**
     * Get the {@link QueryStatistics} related to this query execution.
     */
    QueryStatistics queryStatistics();

    /**
     * Get the {@link QueryProfile} of this query execution.
     */
    QueryProfile queryProfile();

    /**
     * Get the total allocated memory of this query, in bytes.
     *
     * @return the total number of allocated memory bytes, or None, if memory tracking was not enabled.
     */
    Optional<Long> totalAllocatedMemory();

    @Override
    void close();
}
