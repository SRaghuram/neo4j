/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class DriverConfigUtils
{
    public static File neo4jTestConfig( File tempDir ) throws DbException
    {
        try
        {
            File tempConfigFile = new File( tempDir, "temp_neo4j_sf001.conf" );
            Neo4jConfigBuilder.withDefaults()
                              .mergeWith( Neo4jConfigBuilder.fromFile( getResource( "/neo4j/neo4j_sf001.conf" ) ).build() )
                              .writeToFile( tempConfigFile.toPath() );
            return tempConfigFile;
        }
        catch ( Exception e )
        {
            throw new DbException( "Error creating temporary SF1 test config", e );
        }
    }

    public static File getResource( String path )
    {
        return FileUtils.toFile( DriverConfigUtils.class.getResource( path ) );
    }

    public static Map<String,String> ldbcSnbBi() throws IOException
    {
        return LdbcSnbBiWorkloadConfiguration.defaultConfigSF1();
    }

    public static Map<String,String> ldbcSnbInteractive() throws IOException
    {
        return LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
    }
}
