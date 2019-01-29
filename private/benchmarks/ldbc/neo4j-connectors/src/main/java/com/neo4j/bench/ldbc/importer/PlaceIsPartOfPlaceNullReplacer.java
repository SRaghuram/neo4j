/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.importer;

import com.ldbc.driver.csv.charseeker.BufferedCharSeeker;
import com.ldbc.driver.csv.charseeker.Extractors;
import com.ldbc.driver.csv.charseeker.Mark;
import com.ldbc.driver.csv.charseeker.Readables;
import com.ldbc.driver.csv.charseeker.ThreadAheadReadable;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

public class PlaceIsPartOfPlaceNullReplacer
{
    private static char NO_ARRAY_SEPARATOR = ';';
    private static char NO_TUPLE_SEPARATOR = ',';

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
        BufferedCharSeeker mergePlaceIsPartOfPlaceCharSeeker = new BufferedCharSeeker(
                ThreadAheadReadable.threadAhead(
                        Readables.wrap(
                                new InputStreamReader( new FileInputStream( mergePlaceIsPartOfPlace ), charset )
                        ),
                        bufferSize
                ),
                bufferSize
        );
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
            String isPartOfString = mergePlaceIsPartOfPlaceCharSeeker.extract( mark, extractors.string() ).value();
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

        mergePlaceIsPartOfPlaceCharSeeker.close();
    }
}
