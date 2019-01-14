/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.upgrade;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;
import org.neo4j.test.Unzip;

import static java.util.Collections.singletonList;

public class EnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    public EnterpriseStoreUpgraderTest( RecordFormats recordFormats )
    {
        super( recordFormats );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<RecordFormats> versions()
    {
        return singletonList( HighLimitV3_4_0.RECORD_FORMATS );
    }

    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
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
        File directory = databaseLayout.databaseDirectory();
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
        else
        {
            throw new IllegalArgumentException( "Unknown enterprise store version." );
        }
    }

    private static File highLimit3_4Store( File databaseDirectory ) throws IOException
    {
        return Unzip.unzip( EnterpriseStoreUpgraderTest.class, "upgradeTest34HighLimitDb.zip", databaseDirectory );
    }
}
