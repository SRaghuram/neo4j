/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import static org.neo4j.com.storecopy.StoreUtil.cleanStoreDir;
import static org.neo4j.com.storecopy.StoreUtil.deleteRecursive;
import static org.neo4j.com.storecopy.StoreUtil.getBranchedDataRootDirectory;
import static org.neo4j.com.storecopy.StoreUtil.isBranchedDataDirectory;
import static org.neo4j.com.storecopy.StoreUtil.moveAwayDb;
import static org.neo4j.com.storecopy.StoreUtil.newBranchedDataDir;

public enum BranchedDataPolicy
{
    keep_all
            {
                @Override
                public void handle( File databaseDirectory, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );
                    File branchedDataDir = newBranchedDataDir( databaseDirectory );
                    msgLog.debug( "Moving store from " + databaseDirectory + " to " + branchedDataDir );
                    moveAwayDb( databaseDirectory, branchedDataDir );
                }
            },
    keep_last
            {
                @Override
                public void handle( File databaseDirectory, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );

                    File branchedDataDir = newBranchedDataDir( databaseDirectory );
                    msgLog.debug( "Moving store from " + databaseDirectory + " to " + branchedDataDir );
                    moveAwayDb( databaseDirectory, branchedDataDir );
                    File[] files = getBranchedDataRootDirectory( databaseDirectory ).listFiles();
                    if ( files != null )
                    {
                        for ( File file : files )
                        {
                            if ( isBranchedDataDirectory( file ) && !file.equals( branchedDataDir ) )
                            {
                                deleteRecursive( file );
                            }
                        }
                    }
                }
            },
    keep_none
            {
                @Override
                public void handle( File databaseDirectory, PageCache pageCache, LogService logService ) throws IOException
                {
                    Log msgLog = logService.getInternalLog( getClass() );
                    msgLog.debug( "Removing store  " + databaseDirectory );
                    cleanStoreDir( databaseDirectory );
                }
            };

    public abstract void handle( File databaseDirectory, PageCache pageCache, LogService msgLog ) throws IOException;
}
