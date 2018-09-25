/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;

class ReadOnlyFulltextIndex extends ReadOnlyAbstractDatabaseIndex<LuceneFulltextIndex,FulltextIndexReader> implements DatabaseFulltextIndex
{
    ReadOnlyFulltextIndex( LuceneFulltextIndex luceneFulltextIndex )
    {
        super( luceneFulltextIndex );
    }

    @Override
    public TransactionStateLuceneIndexWriter getTransactionalIndexWriter()
    {
        throw new UnsupportedOperationException( "Can't get transaction state index writer for read only lucene index." );
    }
}
