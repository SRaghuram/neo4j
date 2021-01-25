/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.nio.channels.FileChannel;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.util.FeatureToggles;

/**
 * A filesystem which can be configured to avoid flushing to disk, which is useful on
 * multiple platforms (OS X, AWS+EBS) to significantly decrease testing run-times.
 */
public class TestClusterFileSystem extends DefaultFileSystemAbstraction
{
    private static final boolean FORCE = FeatureToggles.flag( TestClusterFileSystem.class, "FORCE", true );

    @Override
    protected StoreFileChannel getStoreFileChannel( FileChannel channel )
    {
        return FORCE ? super.getStoreFileChannel( channel ) : new NoForceStoreFileChannel( channel );
    }

    private static class NoForceStoreFileChannel extends StoreFileChannel
    {
        NoForceStoreFileChannel( FileChannel channel )
        {
            super( channel );
        }

        @Override
        public void force( boolean metaData )
        {
            // ignored
        }
    }
}
