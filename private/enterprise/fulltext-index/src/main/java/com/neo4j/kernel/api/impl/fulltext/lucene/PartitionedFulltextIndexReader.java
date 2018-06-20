/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleFulltextIndexReader}s for individual partitions.
 *
 * @see SimpleFulltextIndexReader
 */
class PartitionedFulltextIndexReader extends FulltextIndexReader
{

    private final List<FulltextIndexReader> indexReaders;

    PartitionedFulltextIndexReader( List<PartitionSearcher> partitionSearchers, String[] properties, Analyzer analyzer )
    {
        this( partitionSearchers.stream().map( partitionSearcher -> new SimpleFulltextIndexReader( partitionSearcher, properties, analyzer ) ).collect(
                Collectors.toList() ) );
    }

    private PartitionedFulltextIndexReader( List<FulltextIndexReader> readers )
    {
        this.indexReaders = readers;
    }

    @Override
    public ScoreEntityIterator query( String query ) throws ParseException
    {
        return partitionedQuery( query );
    }

    @Override
    public void close()
    {
        try
        {
            IOUtils.closeAll( indexReaders );
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    private ScoreEntityIterator partitionedQuery( String query ) throws ParseException
    {
        List<ScoreEntityIterator> results = new ArrayList<>();
        for ( FulltextIndexReader indexReader : indexReaders )
        {
            results.add( indexReader.query( query ) );
        }
        return ScoreEntityIterator.concat( results );
    }
}
