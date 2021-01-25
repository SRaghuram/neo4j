/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import java.io.IOException;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Mark;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.csv.Header;

public class LdbcHeaderFactory implements Header.Factory
{
    private final Header[] headers;
    private int headersIndex;

    public LdbcHeaderFactory( Header... headers )
    {
        this.headers = headers;
    }

    @Override
    public Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups, Header.Monitor monitor )
    {
        Mark mark = new Mark();
        int columnDelimiter = '|';
        do
        {
            try
            {
                dataSeeker.seek( mark, columnDelimiter );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to advance parameters file beyond headers", e );
            }
        }
        while ( !mark.isEndOfLine() );

        Header header = headers[headersIndex % headers.length];

        headersIndex++;
        return header;
    }

    @Override
    public boolean isDefined()
    {
        return false;
    }
}
