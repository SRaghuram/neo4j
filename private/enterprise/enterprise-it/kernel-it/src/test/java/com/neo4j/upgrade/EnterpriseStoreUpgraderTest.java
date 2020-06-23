/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;
import org.neo4j.test.Unzip;

public class EnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    @SuppressWarnings( "unused" )
    private static Collection<RecordFormats> versions()
    {
        return Arrays.asList( HighLimitV3_0_0.RECORD_FORMATS, HighLimitV3_4_0.RECORD_FORMATS );
    }

    @Override
    protected String getRecordFormatsName()
    {
        return HighLimit.NAME;
    }

    @Override
    protected void prepareSampleDatabase( String version, FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
            File databaseDirectory ) throws IOException
    {
        File resourceDirectory = findFormatStoreDirectoryForVersion( version, databaseDirectory );
        File directory = databaseLayout.databaseDirectory().toFile();
        fileSystem.deleteRecursively( directory );
        fileSystem.mkdirs( directory );
        fileSystem.copyRecursively( resourceDirectory, directory );
    }

    private static File findFormatStoreDirectoryForVersion( String version, File databaseDirectory ) throws IOException
    {
        if ( version.equals( HighLimitV3_4_0.STORE_VERSION ) )
        {
            return highLimit3_4Store( databaseDirectory );
        }
        if ( version.equals( HighLimitV3_0_0.STORE_VERSION ) )
        {
            return highLimit3_0Store( databaseDirectory );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown enterprise store version." );
        }
    }

    private static File highLimit3_0Store( File databaseDirectory ) throws IOException
    {
        return Unzip.unzip( EnterpriseStoreUpgraderTest.class, "upgradeTest30HighLimitDb.zip", databaseDirectory );
    }

    private static File highLimit3_4Store( File databaseDirectory ) throws IOException
    {
        return Unzip.unzip( EnterpriseStoreUpgraderTest.class, "upgradeTest34HighLimitDb.zip", databaseDirectory );
    }
}
