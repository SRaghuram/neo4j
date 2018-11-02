/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class BranchedDataMigrator extends LifecycleAdapter
{
    private final File storeDir;

    public BranchedDataMigrator( File storeDir )
    {
        this.storeDir = storeDir;
    }

    @Override
    public void start()
    {
        migrateBranchedDataDirectoriesToRootDirectory();
    }

    private void migrateBranchedDataDirectoriesToRootDirectory()
    {
        File branchedDir = StoreUtil.getBranchedDataRootDirectory( storeDir );
        branchedDir.mkdirs();
        for ( File oldBranchedDir : storeDir.listFiles() )
        {
            if ( !oldBranchedDir.isDirectory() || !oldBranchedDir.getName().startsWith( "branched-" ) )
            {
                continue;
            }

            long timestamp = 0;
            try
            {
                timestamp = Long.parseLong( oldBranchedDir.getName().substring( oldBranchedDir.getName().indexOf( '-'
                ) + 1 ) );
            }
            catch ( NumberFormatException e )
            {   // OK, it wasn't a branched directory after all.
                continue;
            }

            File targetDir = StoreUtil.getBranchedDataDirectory( storeDir, timestamp );
            try
            {
                FileUtils.moveFile( oldBranchedDir, targetDir );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Couldn't move branched directories to " + branchedDir, e );
            }
        }
    }
}
