/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package batchimport;

import java.util.function.Function;

import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporterTest;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Groups;

/**
 * Test for {@link ParallelBatchImporter} in an enterprise environment so that enterprise store is used.
 */
public class HighLimitParallelBatchImporterIT extends ParallelBatchImporterTest
{
    public HighLimitParallelBatchImporterIT( InputIdGenerator inputIdGenerator, Function<Groups,IdMapper> idMapper )
    {
        super( inputIdGenerator, idMapper );
    }

    @Override
    public RecordFormats getFormat()
    {
        return HighLimit.RECORD_FORMATS;
    }
}
