/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import com.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;

import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.core.TokenHolder;

class ReadOnlyFulltextIndex extends ReadOnlyAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader>
{
    ReadOnlyFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder )
    {
        super( new LuceneFulltextIndex( storage, partitionFactory, descriptor, propertyKeyTokenHolder ) );
    }
}
