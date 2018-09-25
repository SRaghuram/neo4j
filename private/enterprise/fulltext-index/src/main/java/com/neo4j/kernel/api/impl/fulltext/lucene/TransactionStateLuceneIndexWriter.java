/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.Closeable;
import java.io.IOException;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;

public class TransactionStateLuceneIndexWriter implements LuceneIndexWriter, Closeable
{
    private final LuceneFulltextIndex index;
    private IndexWriter writer;
    private final Directory directory;

    TransactionStateLuceneIndexWriter( LuceneFulltextIndex index )
    {
        this.index = index;
        directory = new RAMDirectory(); //index.createIndexTransactionDirectory();
    }

    @Override
    public void addDocument( Document document ) throws IOException
    {
        writer.addDocument( document );
    }

    @Override
    public void addDocuments( int numDocs, Iterable<Document> document ) throws IOException
    {
        writer.addDocuments( document );
    }

    @Override
    public void updateDocument( Term term, Document document ) throws IOException
    {
        writer.updateDocument( term, document );
    }

    @Override
    public void deleteDocuments( Term term ) throws IOException
    {
        writer.deleteDocuments( term );
    }

    @Override
    public void deleteDocuments( Query query ) throws IOException
    {
        writer.deleteDocuments( query );
    }

    public void resetWriterState() throws IOException
    {
        if ( writer != null )
        {
            // Note that 'rollback' closes the writer.
            writer.rollback();
        }
        openWriter();
    }

    private void openWriter() throws IOException
    {
        writer = new IndexWriter( directory, index.createIndexWriterConfig() );
    }

    public FulltextIndexReader getNearRealTimeReader() throws IOException
    {
        DirectoryReader directoryReader = DirectoryReader.open( writer, true );
        IndexSearcher searcher = new IndexSearcher( directoryReader );
        SearcherReference searcherRef = new DirectSearcherReference( searcher, directoryReader );
        return new SimpleFulltextIndexReader( searcherRef, index.getPropertiesArray(), index.getAnalyzer(), index.getPropertyKeyTokenHolder() );
    }

    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( (AutoCloseable) writer::rollback, directory );
    }
}
