/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.consistency;

import org.apache.commons.lang3.SystemUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.internal.locker.FileLockException;
import org.neo4j.kernel.recovery.Recovery;
import org.neo4j.logging.Log;
import org.neo4j.memory.EmptyMemoryTracker;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.FormattedLogProvider.toOutputStream;

public class RobustnessConsistencyCheck
{
    private final Log log;

    public RobustnessConsistencyCheck( Log log )
    {
        this.log = log;
    }

    public void verifyConsistencyOffline( File homeDir ) throws Exception
    {
        DatabaseLayout databaseLayout = Neo4jLayout.of( homeDir.toPath() ).databaseLayout( DEFAULT_DATABASE_NAME );

        recoverDatabase( databaseLayout );

        try
        {
            Map<String,String> params = stringMap( "--check-property-owners", "true" ); //what kind of settings are these?

            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout,
                    Config.newBuilder().setRaw( params ).build(), ProgressMonitorFactory.NONE, toOutputStream( System.out ), false, ConsistencyFlags.DEFAULT );

            if ( !result.isSuccessful() )
            {
                log.error( "FATAL: Found inconsistencies in: " + homeDir );
                throw new RuntimeException( "KHER-HOOOGA .. KHER-HOOGA: Corruption detected in: " + homeDir );
            }
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void recoverDatabase( DatabaseLayout databaseLayout ) throws Exception
    {
        try
        {
            log.info( "Ensuring database is recovered (" + databaseLayout + ")" );

            Recovery.performRecovery( databaseLayout, DatabaseTracers.EMPTY, EmptyMemoryTracker.INSTANCE );

            log.info( "Done ensuring database is recovered (" + databaseLayout + ")" );
        }
        catch ( RuntimeException e )
        {
            if ( storeIsLocked( e ) && !SystemUtils.IS_OS_WINDOWS )
            {
                log.error( "FATAL: Unable to open database for verification, dumping lsof for debugging. " );
                lsof( databaseLayout.databaseDirectory().toFile() );
                lsof( databaseLayout.getTransactionLogsDirectory().toFile() );
            }
            throw new IllegalStateException( "Unable to start database for verification!", e );
        }
    }

    private void lsof( File dir )
    {
        try ( InputStream in = new ProcessBuilder( "lsof", dir.getAbsolutePath() ).start().getInputStream() )
        {
            BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
            String line;
            while ( (line = br.readLine()) != null )
            {
                log.error( line );
            }
        }
        catch ( IOException e )
        {
            log.error( "Unable to dump lsof", e );
        }
    }

    private boolean storeIsLocked( RuntimeException e )
    {
        return e.getCause() != null && e.getCause().getCause() != null && e.getCause().getCause() instanceof FileLockException;
    }
}

