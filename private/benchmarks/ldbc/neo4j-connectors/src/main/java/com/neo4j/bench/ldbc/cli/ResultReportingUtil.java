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

package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.ClassLoadingException;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.client.model.Neo4jConfig;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_store_upgrade;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.record_format;

class ResultReportingUtil
{
    // Benchmark Group Name Format: 'Workload' + 'Read/Write' + 'Scale Factor'
    static String toBenchmarkGroupName( DriverConfiguration ldbcConfig ) throws WorkloadException
    {
        Workload workload = ClassLoaderHelper.loadWorkload( ldbcConfig.workloadClassName() );
        String group = workload.getClass().getSimpleName().replace( "Workload", "" );
        if ( hasReads( ldbcConfig ) )
        {
            group += "-Read";
        }
        if ( hasWrites( ldbcConfig ) )
        {
            group += "-Write";
        }
        return group;
    }

    static boolean hasReads( DriverConfiguration ldbcConfig ) throws WorkloadException
    {
        Workload workload = ClassLoaderHelper.loadWorkload( ldbcConfig.workloadClassName() );
        // Interactive Workload
        if ( workload instanceof LdbcSnbInteractiveWorkload )
        {
            return LdbcSnbInteractiveWorkloadConfiguration.hasReads( ldbcConfig.asMap() );
        }
        // Business Intelligence Workload
        else if ( workload instanceof LdbcSnbBiWorkload )
        {
            return LdbcSnbBiWorkloadConfiguration.hasReads( ldbcConfig.asMap() );
        }
        else
        {
            throw new RuntimeException( "Unrecognized workload: " + workload.getClass().getName() );
        }
    }

    static boolean hasWrites( DriverConfiguration ldbcConfig ) throws WorkloadException
    {
        Workload workload = ClassLoaderHelper.loadWorkload( ldbcConfig.workloadClassName() );
        // Interactive Workload
        if ( workload instanceof LdbcSnbInteractiveWorkload )
        {
            return LdbcSnbInteractiveWorkloadConfiguration.hasWrites( ldbcConfig.asMap() );
        }
        // Business Intelligence Workload
        else if ( workload instanceof LdbcSnbBiWorkload )
        {
            return LdbcSnbBiWorkloadConfiguration.hasWrites( ldbcConfig.asMap() );
        }
        else
        {
            throw new RuntimeException( "Unrecognized workload: " + workload.getClass().getName() );
        }
    }

    static int extractScaleFactor( File readParametersDir )
    {
        // E.g., ldbc_sf001_p006_regular_utc
        String datasetName = readParametersDir.getParentFile().toPath().getFileName().toString();

        String regex = "^ldbc\\_sf\\d{1,4}\\_.+";
        Pattern pattern = Pattern.compile( regex );
        if ( !pattern.matcher( datasetName ).matches() )
        {
            throw new RuntimeException( "Dataset name did not conform to regex: " + regex );
        }

        // E.g., sf001_p006_regular_utc
        String withoutLdbcPrefix = datasetName.substring( datasetName.indexOf( "_sf" ) + 1 );
        // E.g., 001
        String scaleFactor = withoutLdbcPrefix.substring( 2, withoutLdbcPrefix.indexOf( "_" ) );
        // E.g., 1
        return Integer.parseInt( scaleFactor );
    }

    static void assertDisallowFormatMigration( File neo4jConfigFile )
    {
        assertSettingProvided( neo4jConfigFile, allow_upgrade.name() );
    }

    static void assertStoreFormatIsSet( File neo4jConfigFile )
    {
        assertSettingProvided( neo4jConfigFile, record_format.name() );
    }

    private static void assertSettingProvided( File neo4jConfigFile, String setting )
    {
        try
        {
            Neo4jConfig neo4jConfig = Neo4jConfig.fromFile( neo4jConfigFile );
            if ( !neo4jConfig.toMap().containsKey( setting ) ||
                 neo4jConfig.toMap().get( setting ).equals( "true" ) )
            {
                throw new RuntimeException( setting + " must be disabled" );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving Neo4j configuration", e );
        }
    }
}
