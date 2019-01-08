/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Path;

/**
 * {@link UniquenessFilter} for {@link BidirectionalTraversalDescription bidirectional traverser}.
 */
public interface BidirectionalUniquenessFilter extends UniquenessFilter
{
    /**
     * Checks {@link Path} alone to see if it follows the uniqueness contract
     * provided by this {@link UniquenessFilter}.
     *
     * @param path the {@link Path} to examine.
     * @return {@code true} if the {@code path} fulfills the uniqueness contract,
     *         otherwise {@code false}.
     */
    boolean checkFull( Path path );
}
