/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.PathExpander;

/**
 * Selects {@link TraversalBranch}s according to postorder depth first pattern,
 * see http://en.wikipedia.org/wiki/Depth-first_search
 */
class PostorderDepthFirstSelector implements BranchSelector
{
    private TraversalBranch current;
    private final PathExpander expander;

    PostorderDepthFirstSelector( TraversalBranch startSource, PathExpander expander )
    {
        this.current = startSource;
        this.expander = expander;
    }

    @Override
    public TraversalBranch next( TraversalContext metadata )
    {
        TraversalBranch result = null;
        while ( result == null )
        {
            if ( current == null )
            {
                return null;
            }

            TraversalBranch next = current.next( expander, metadata );
            if ( next != null )
            {
                current = next;
            }
            else
            {
                result = current;
                current = current.parent();
            }
        }
        return result;
    }
}
