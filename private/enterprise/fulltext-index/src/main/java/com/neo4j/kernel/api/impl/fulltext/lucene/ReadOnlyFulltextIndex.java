/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.apache.lucene.analysis.Analyzer;

import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

class ReadOnlyFulltextIndex extends ReadOnlyAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader> implements FulltextIndex
{
    ReadOnlyFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, Analyzer analyzer, FulltextIndexDescriptor descriptor )
    {
        super( new LuceneFulltextIndex( storage, partitionFactory, analyzer, descriptor ) );
    }
}
