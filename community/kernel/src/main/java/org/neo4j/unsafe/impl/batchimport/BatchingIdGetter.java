/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.id.RenewableBatchIdSequences;
import org.neo4j.kernel.impl.store.id.validation.IdValidator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Exposes batches of ids from a {@link RecordStore} as a {@link PrimitiveLongIterator}.
 * It makes use of {@link IdSequence#nextIdBatch(int)} (with default batch size the number of records per page)
 * and caches that batch, exhausting it in {@link #next()} before getting next batch.
 *
 * TODO use the {@link RenewableBatchIdSequences} instead.
 */
public class BatchingIdGetter extends PrimitiveLongCollections.PrimitiveLongBaseIterator
{
    private final IdSequence source;
    private IdRangeIterator batch;
    private final int batchSize;

    public BatchingIdGetter( RecordStore<? extends AbstractBaseRecord> source )
    {
        this( source, source.getRecordsPerPage() );
    }

    public BatchingIdGetter( RecordStore<? extends AbstractBaseRecord> source, int batchSize )
    {
        this.source = source;
        this.batchSize = batchSize;
    }

    @Override
    protected boolean fetchNext()
    {
        long id;
        if ( batch == null || (id = batch.nextId()) == -1 )
        {
            IdRange idRange = source.nextIdBatch( batchSize );
            while ( IdValidator.hasReservedIdInRange( idRange.getRangeStart(), idRange.getRangeStart() + idRange.getRangeLength() ) )
            {
                idRange = source.nextIdBatch( batchSize );
            }
            batch = new IdRangeIterator( idRange );
            id = batch.nextId();
        }
        return next( id );
    }
}
