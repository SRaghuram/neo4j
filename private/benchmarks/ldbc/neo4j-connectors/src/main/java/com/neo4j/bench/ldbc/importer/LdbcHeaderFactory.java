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

import java.io.IOException;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Mark;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

public class LdbcHeaderFactory implements Header.Factory
{
    private final Header[] headers;
    private int headersIndex;

    public LdbcHeaderFactory( Header... headers )
    {
        this.headers = headers;
    }

    @Override
    public Header create( CharSeeker dataSeeker, org.neo4j.unsafe.impl.batchimport.input.csv.Configuration
            configuration, IdType idType )
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
        while ( false == mark.isEndOfLine() );

        Header header = headers[headersIndex % headers.length];

        headersIndex++;
        return header;
    }
}
