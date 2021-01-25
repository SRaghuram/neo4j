/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.csv.simple.SimpleCsvFileReader;

import java.io.File;
import java.io.FileNotFoundException;

public class CsvFileInserter
{
    private final File file;
    private final SimpleCsvFileReader csvReader;
    private final CsvLineInserter lineInserter;
    // first line == 0
    private final int startLine;

    public CsvFileInserter(
            File file,
            CsvLineInserter lineInserter )
    {
        this( file, lineInserter, 1 );
    }

    private CsvFileInserter(
            File file,
            CsvLineInserter lineInserter,
            int startLine )
    {
        try
        {
            this.file = file;
            this.csvReader = new SimpleCsvFileReader( file, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            this.lineInserter = lineInserter;
            this.startLine = startLine;
            advanceCsvReaderToStartLine();
        }
        catch ( FileNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    public File getFile()
    {
        return file;
    }

    private void advanceCsvReaderToStartLine()
    {
        for ( int i = 0; i < startLine; i++ )
        {
            if ( csvReader.hasNext() )
            {
                csvReader.next();
            }
            else
            {
                return;
            }
        }
    }

    public int insertAll() throws FileNotFoundException
    {
        int count = 0;
        while ( csvReader.hasNext() )
        {
            lineInserter.insert( csvReader.next() );
            count++;
        }
        csvReader.close();
        return count;
    }
}
