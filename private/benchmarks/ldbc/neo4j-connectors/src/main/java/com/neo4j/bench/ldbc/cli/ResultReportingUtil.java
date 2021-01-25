/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.cli;

import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkload;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiWorkloadConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.ldbc.utils.StoreFormat;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;

import java.io.File;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.kernel.impl.store.format.standard.Standard;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

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

    public static StoreFormat extractStoreFormat( Neo4jConfig neo4jConfig )
    {
        String formatString = neo4jConfig.toMap().get( record_format.name() );
        switch ( formatString )
        {
        case Standard.LATEST_NAME:
            return StoreFormat.STANDARD;
        case HighLimit.NAME:
            return StoreFormat.HIGH_LIMIT;
        default:
            throw new RuntimeException( "Unexpected record format found: " + formatString );
        }
    }

    public static int extractScaleFactor( File readParametersDir )
    {
        // E.g., ldbc_sf001_p006_regular_utc
        String datasetName = readParametersDir.getParentFile().toPath().getFileName().toString();

        String regex = "^ldbc_sf\\d{1,4}_.+";
        Pattern pattern = Pattern.compile( regex );
        if ( !pattern.matcher( datasetName ).matches() )
        {
            throw new RuntimeException( format( "Dataset name did not conform to regex: %s\n" +
                                                "Dataset name:                          %s\n" +
                                                "Full path:                             %s",
                                                regex, datasetName, readParametersDir.getAbsolutePath() ) );
        }

        // E.g., sf001_p006_regular_utc
        String withoutLdbcPrefix = datasetName.substring( datasetName.indexOf( "_sf" ) + 1 );
        // E.g., 001
        String scaleFactor = withoutLdbcPrefix.substring( 2, withoutLdbcPrefix.indexOf( '_' ) );
        // E.g., 1
        return Integer.parseInt( scaleFactor );
    }

    static void assertDisallowFormatMigration( Neo4jConfig neo4jConfig )
    {
        Map<String,String> neo4jConfigMap = assertSettingProvided( neo4jConfig, allow_upgrade.name() );
        if ( neo4jConfigMap.get( allow_upgrade.name() ).equals( "true" ) )
        {
            throw new RuntimeException( allow_upgrade.name() + " must be disabled" );
        }
    }

    static void assertStoreFormatIsSet( Neo4jConfig neo4jConfig )
    {
        assertSettingProvided( neo4jConfig, record_format.name() );
    }

    private static Map<String,String> assertSettingProvided( Neo4jConfig neo4jConfig, String setting )
    {
        try
        {
            Map<String,String> neo4jConfigMap = neo4jConfig.toMap();
            if ( !neo4jConfigMap.containsKey( setting ) )
            {
                throw new RuntimeException( setting + " must be provided" );
            }
            return neo4jConfigMap;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error retrieving Neo4j configuration", e );
        }
    }
}
