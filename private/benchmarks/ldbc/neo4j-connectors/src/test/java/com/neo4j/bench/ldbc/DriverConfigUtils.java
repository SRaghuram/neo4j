/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.DbException;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class DriverConfigUtils
{
    public static File neo4jTestConfig() throws DbException
    {
        try
        {
            Map<String,String> config = MapUtils.loadPropertiesToMap( getResource( "/neo4j/neo4j_sf001.conf" ) );
            // nothing to change for 2.3+
            File tempConfigFile = File.createTempFile( "temp_neo4j_sf001", "conf" );
            MapUtils.mapToProperties( config ).store( new FileOutputStream( tempConfigFile ), "Test Config" );
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
