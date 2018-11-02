/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public interface ConsistencyCheck
{
    ConsistencyCheck NONE = new ConsistencyCheck()
            {
                @Override
                public String name()
                {
                    return "none";
                }

                @Override
                public boolean runFull( DatabaseLayout databaseLayout, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags )
                {
                    return true;
                }
            };

    ConsistencyCheck FULL = new ConsistencyCheck()
            {
                @Override
                public String name()
                {
                    return "full";
                }

                @Override
                public boolean runFull( DatabaseLayout databaseLayout, Config tuningConfiguration,
                        ProgressMonitorFactory progressFactory, LogProvider logProvider,
                        FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                        ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException
                {
                    try
                    {
                        return new ConsistencyCheckService().runFullConsistencyCheck(
                                databaseLayout, tuningConfiguration, progressFactory, logProvider, fileSystem,
                                pageCache, verbose, consistencyFlags ).isSuccessful();
                    }
                    catch ( ConsistencyCheckIncompleteException e )
                    {
                        throw new ConsistencyCheckFailedException( e );
                    }
                }
            };

    String name();

    boolean runFull( DatabaseLayout databaseLayout, Config tuningConfiguration, ProgressMonitorFactory progressFactory,
                     LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache, boolean verbose,
                     ConsistencyFlags consistencyFlags ) throws ConsistencyCheckFailedException;

    String toString();

    static ConsistencyCheck fromString( String name )
    {
        for ( ConsistencyCheck consistencyCheck : new ConsistencyCheck[]{NONE, FULL} )
        {
            if ( consistencyCheck.name().equalsIgnoreCase( name ) )
            {
                return consistencyCheck;
            }
        }
        throw new IllegalArgumentException( "Unknown consistency check name: " + name +
                ". Supported values: NONE, FULL" );
    }
}
