/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.CharReadable;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import static com.ldbc.driver.csv.charseeker.ThreadAheadReadable.threadAhead;

public class PlaceIsPartOfPlaceNullReplacer
{
    private static final char NO_ARRAY_SEPARATOR = ';';
    private static final char NO_TUPLE_SEPARATOR = ',';

    // place is part of place
    // places: id|name|url|type|isPartOf|
    public void replaceNullsWithSelfReferencingRelationships(
            List<File> mergePlaceIsPartOfPlaces,
            Charset charset,
            char columnDelimiter,
            File newMergePlaceIsPartOfPlace ) throws IOException
    {
        com.ldbc.driver.util.FileUtils.tryCreateFile( newMergePlaceIsPartOfPlace, false );
        SimpleCsvFileWriter simpleCsvFileWriter = new SimpleCsvFileWriter(
                newMergePlaceIsPartOfPlace,
                Character.toString( columnDelimiter ) );
        String[] row = new String[5];
        row[0] = "id";
        row[1] = "name";
        row[2] = "url";
        row[3] = "type";
        row[4] = "isPartOf";
        simpleCsvFileWriter.writeRow( row );
        for ( File mergePlaceIsPartOfPlace : mergePlaceIsPartOfPlaces )
        {
            replaceNullsWithSelfReferencingRelationships(
                    mergePlaceIsPartOfPlace,
                    charset,
                    columnDelimiter,
                    simpleCsvFileWriter );
        }
        simpleCsvFileWriter.close();
    }

    private void replaceNullsWithSelfReferencingRelationships(
            File mergePlaceIsPartOfPlace,
            Charset charset,
            char columnDelimiter,
            SimpleCsvFileWriter simpleCsvFileWriter ) throws IOException
    {
        int bufferSize = 1 * 1024 * 1024;

        try ( InputStreamReader fileReader =
                      new InputStreamReader( new FileInputStream( mergePlaceIsPartOfPlace ), charset ) )
        {
            try ( CharReadable threadAheadReader = threadAhead( Readables.wrap( fileReader ), bufferSize ) )
            {
                try ( BufferedCharSeeker mergePlaceIsPartOfPlaceCharSeeker =
                              new BufferedCharSeeker( threadAheadReader, bufferSize ) )
                {
                    Extractors extractors = new Extractors( NO_ARRAY_SEPARATOR, NO_TUPLE_SEPARATOR );
                    Mark mark = new Mark();
                    int[] columnDelimiters = new int[]{columnDelimiter};
                    // skip headers
                    while ( !mark.isEndOfLine() )
                    {
                        mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                    }
                    String[] row = new String[5];

                    // read: id
                    while ( mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters ) )
                    {
                        long id = mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.long_() ).longValue();
                        // read: name
                        mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                        String name = mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.string() ).value();
                        // read: url
                        mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                        String url = mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.string() ).value();
                        // read: type
                        mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                        String type = mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.string() ).value();
                        // read: isPartOf
                        mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                        String isPartOfString =
                                mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.string() ).value();
                        long isPartOf = (null == isPartOfString) ? id : Long.parseLong( isPartOfString );

                        row[0] = Long.toString( id );
                        row[1] = name;
                        row[2] = url;
                        row[3] = type;
                        row[4] = Long.toString( isPartOf );

                        simpleCsvFileWriter.writeRow( row );

                        // read to end of line, ignoring remaining columns <-- ignore end-of-row separator
                        while ( !mark.isEndOfLine() )
                        {
                            mergePlaceIsPartOfPlaceCharSeeker.seek( mark, columnDelimiters );
                        }
                    }
                }
            }
        }
    }
}
