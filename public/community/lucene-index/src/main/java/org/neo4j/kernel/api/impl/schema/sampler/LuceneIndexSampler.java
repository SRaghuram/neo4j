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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.neo4j.internal.helpers.TaskControl;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexSampler;

/**
 * Abstract implementation of a Lucene index sampler, that can react on sampling being canceled via
 * {@link TaskCoordinator#cancel()} }.
 */
abstract class LuceneIndexSampler implements IndexSampler
{
    private final TaskControl executionTicket;

    LuceneIndexSampler( TaskControl taskControl )
    {
        this.executionTicket = taskControl;
    }

    /**
     * Check if sampling was canceled.
     *
     * @throws IndexNotFoundKernelException if cancellation was requested.
     */
    void checkCancellation() throws IndexNotFoundKernelException
    {
        if ( executionTicket.cancellationRequested() )
        {
            throw new IndexNotFoundKernelException( "Index dropped while sampling." );
        }
    }

    @Override
    public void close()
    {
        executionTicket.close();
    }
}
