/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.index.schema.config;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Function;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.io.pagecache.PageCache;

/**
 * <p>
 * This factory can be used to create new space filling curve settings for use in configuring the curves.
 * These settings can be created either by defaults from the neo4j.conf file (see ConfiguredSpaceFullCurveSettingsCache)
 * or from reading the header of an existing GBPTree based index.
 */
public final class SpaceFillingCurveSettingsFactory
{
    private SpaceFillingCurveSettingsFactory()
    {
    }

    /**
     * This method builds the default index configuration object for the specified CRS and other config options.
     * Currently we only support a SingleSpaceFillingCurveSettings which is the best option for cartesian, but
     * not necessarily the best for geographic coordinate systems.
     */
    static SpaceFillingCurveSettings fromConfig( int maxBits, EnvelopeSettings envelopeSettings )
    {
        // Currently we support only one type of index, but in future we could support different types for different CRS
        return new SpaceFillingCurveSettings.SettingsFromConfig( envelopeSettings.getCrs().getDimension(), maxBits, envelopeSettings.asEnvelope() );
    }

    public static SpaceFillingCurveSettings fromGBPTree( File indexFile, PageCache pageCache, Function<ByteBuffer,String> onError ) throws IOException
    {
        SpaceFillingCurveSettings.SettingsFromIndexHeader settings = new SpaceFillingCurveSettings.SettingsFromIndexHeader();
        GBPTree.readHeader( pageCache, indexFile, settings.headerReader( onError ) );
        if ( settings.isFailed() )
        {
            throw new IOException( settings.getFailureMessage() );
        }
        return settings;
    }
}
