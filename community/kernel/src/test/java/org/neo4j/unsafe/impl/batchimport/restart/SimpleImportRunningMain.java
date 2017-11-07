/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.restart;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;

public class SimpleImportRunningMain
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 1_000;

    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Starting import" );
        try
        {
            new RestartableParallelBatchImporter( new File( args[0] ), new DefaultFileSystemAbstraction(), null, DEFAULT, NullLogService.getInstance(),
                    ExecutionMonitors.defaultVisible(), EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat() ).doImport(
                    input( Long.parseLong( args[1] ) ) );
        }
        catch ( IllegalStateException e )
        {
            if ( e.getMessage().contains( "already contains data, cannot do import here" ) )
            {
                System.out.println( "Import done" );
            }
            else
            {
                throw e;
            }
        }
    }

    private static SimpleRandomizedInput input( long seed )
    {
        return new SimpleRandomizedInput( seed, NODE_COUNT, RELATIONSHIP_COUNT, 0, 0 );
    }
}
