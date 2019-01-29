/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
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
