/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;

import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;

@SuppressWarnings( {"SameParameterValue", "unused"} )
public class Config
{
    private static final String ENV_OVERRIDE_PREFIX = "STRESS_TESTING_";

    /* platform */
    private LogProvider logProvider;
    private String workingDir;

    /* general */
    private int numberOfCores;
    private int numberOfEdges;

    private boolean raftMessagesLog;

    private int workDurationMinutes;
    private int shutdownDurationMinutes;

    private String txPrune;
    private String checkpointPolicy;

    private Collection<Preparations> preparations;
    private Collection<Workloads> workloads;
    private Collection<Validations> validations;

    /* workload specific */
    private boolean enableIndexes;
    private long reelectIntervalSeconds;
    private int numberOfDatabases;

    public Config()
    {
        logProvider = new Log4jLogProvider( System.out );
        workingDir = envOrDefault( "WORKING_DIR", new File( getProperty( "java.io.tmpdir" ) ).getPath() );

        numberOfCores = envOrDefault( "NUMBER_OF_CORES", 3 );
        numberOfEdges = envOrDefault( "NUMBER_OF_EDGES", 1 );

        raftMessagesLog = envOrDefault( "ENABLE_RAFT_MESSAGES_LOG", false );

        workDurationMinutes = envOrDefault( "WORK_DURATION_MINUTES", 30 );
        shutdownDurationMinutes = envOrDefault( "SHUTDOWN_DURATION_MINUTES", 5 );

        txPrune = envOrDefault( "TX_PRUNE", "50 files" );
        checkpointPolicy = envOrDefault( "CHECKPOINT_POLICY", GraphDatabaseSettings.check_point_policy.defaultValue().name() );

        preparations = envOrDefault( Preparations.class, "PREPARATIONS" );
        workloads = envOrDefault( Workloads.class, "WORKLOADS" );
        validations = envOrDefault( Validations.class, "VALIDATIONS", Validations.ConsistencyCheck );

        enableIndexes = envOrDefault( "ENABLE_INDEXES", false );
        reelectIntervalSeconds = envOrDefault( "REELECT_INTERVAL_SECONDS", 60L );
        numberOfDatabases = envOrDefault( "NUMBER_OF_DATABASES", 100 );
    }

    private static String envOrDefault( String name, String defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).orElse( defaultValue );
    }

    private static int envOrDefault( String name, int defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Integer::parseInt ).orElse( defaultValue );
    }

    private static long envOrDefault( String name, long defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Long::parseLong ).orElse( defaultValue );
    }

    private static boolean envOrDefault( String name, boolean defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( Boolean::parseBoolean ).orElse( defaultValue );
    }

    @SafeVarargs
    private static <T extends Enum<T>> Collection<T> envOrDefault( Class<T> type, String name, T... defaultValue )
    {
        String environmentVariableName = ENV_OVERRIDE_PREFIX + name;
        return ofNullable( getenv( environmentVariableName ) ).map( env -> parseEnum( env, type ) ).orElse( asList( defaultValue ) );
    }

    private static <T extends Enum<T>> Collection<T> parseEnum( String value, Class<T> type )
    {
        if ( value == null || value.length() == 0 )
        {
            return emptyList();
        }

        ArrayList<T> workloads = new ArrayList<>();
        String[] split = value.split( "," );
        for ( String workloadString : split )
        {
            workloads.add( Enum.valueOf( type, workloadString ) );
        }
        return workloads;
    }

    public LogProvider logProvider()
    {
        return logProvider;
    }

    public void logProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public String workingDir()
    {
        return workingDir;
    }

    public int numberOfCores()
    {
        return numberOfCores;
    }

    public int numberOfEdges()
    {
        return numberOfEdges;
    }

    public void numberOfEdges( int numberOfEdges )
    {
        this.numberOfEdges = numberOfEdges;
    }

    public void workDurationMinutes( int workDurationMinutes )
    {
        this.workDurationMinutes = workDurationMinutes;
    }

    public int workDurationMinutes()
    {
        return workDurationMinutes;
    }

    public int shutdownDurationMinutes()
    {
        return shutdownDurationMinutes;
    }

    public void preparations( Preparations... preparations )
    {
        this.preparations = asList( preparations );
    }

    public Collection<Preparations> preparations()
    {
        return preparations;
    }

    public void workloads( Workloads... workloads )
    {
        this.workloads = asList( workloads );
    }

    public Collection<Workloads> workloads()
    {
        return workloads;
    }

    public void validations( Validations... validations )
    {
        this.validations = asList( validations );
    }

    public Collection<Validations> validations()
    {
        return validations;
    }

    public boolean enableIndexes()
    {
        return enableIndexes;
    }

    public long reelectIntervalSeconds()
    {
        return reelectIntervalSeconds;
    }

    public void reelectIntervalSeconds( int reelectIntervalSeconds )
    {
        this.reelectIntervalSeconds = reelectIntervalSeconds;
    }

    public int numberOfDatabases()
    {
        return numberOfDatabases;
    }

    public void numberOfDatabases( int numberOfDatabases )
    {
        this.numberOfDatabases = numberOfDatabases;
    }

    private void populateCommonParams( Map<String,String> params )
    {
        params.put( GraphDatabaseSettings.keep_logical_logs.name(), txPrune );
        params.put( GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M" );
        params.put( GraphDatabaseSettings.check_point_policy.name(), checkpointPolicy );
    }

    public void populateCoreParams( Map<String,String> params )
    {
        populateCommonParams( params );

        params.put( CausalClusteringSettings.raft_log_rotation_size.name(), "10M" );
        params.put( CausalClusteringSettings.raft_log_pruning_frequency.name(), "1m" );
        // the following will override the test-default in CoreClusterMember
        params.put( CausalClusteringInternalSettings.raft_messages_log_enable.name(), Boolean.toString( raftMessagesLog ) );
    }

    public void populateReadReplicaParams( Map<String,String> params )
    {
        populateCommonParams( params );
    }

    @Override
    public String toString()
    {
        return "Config{workingDir='" + workingDir + '\'' + ", numberOfCores=" +
               numberOfCores + ", numberOfEdges=" + numberOfEdges + ", raftMessagesLog=" + raftMessagesLog + ", workDurationMinutes=" + workDurationMinutes +
               ", shutdownDurationMinutes=" + shutdownDurationMinutes + ", txPrune='" + txPrune + '\'' + ", checkpointPolicy='" + checkpointPolicy + '\'' +
               ", preparations=" + preparations + ", workloads=" + workloads + ", validations=" + validations + ", enableIndexes=" + enableIndexes +
               ", reelectIntervalSeconds=" + reelectIntervalSeconds + ", numberOfDatabases=" + numberOfDatabases + '}';
    }
}
