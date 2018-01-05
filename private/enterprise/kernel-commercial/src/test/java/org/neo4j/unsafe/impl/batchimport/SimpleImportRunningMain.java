/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

public class SimpleImportRunningMain
{
    private static final int NODE_COUNT = 100;
    private static final int RELATIONSHIP_COUNT = 1_000;

    public static void main( String[] args ) throws IOException
    {
        try
        {
            BatchImporterFactory.withHighestPriority().instantiate( new File( args[0] ), new DefaultFileSystemAbstraction(),
                    null, DEFAULT, NullLogService.getInstance(),
                    ExecutionMonitors.invisible(), EMPTY, Config.defaults(), RecordFormatSelector.defaultFormat(), NO_MONITOR ).doImport(
                    input( Long.parseLong( args[1] ) ) );
        }
        catch ( IllegalStateException e )
        {
            if ( e.getMessage().contains( "already contains data, cannot do import here" ) )
            {   // In this test, this exception is OK
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
