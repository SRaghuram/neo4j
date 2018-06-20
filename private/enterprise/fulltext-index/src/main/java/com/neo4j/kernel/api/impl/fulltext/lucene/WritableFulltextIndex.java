/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.analysis.Analyzer;

import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

class WritableFulltextIndex extends WritableAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader> implements FulltextIndex
{
    WritableFulltextIndex( PartitionedIndexStorage storage, WritableIndexPartitionFactory partitionFactory, Analyzer analyzer,
            FulltextIndexDescriptor descriptor )
    {
        super( new LuceneFulltextIndex( storage, partitionFactory, analyzer, descriptor ) );
    }
}
