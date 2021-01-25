/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.result_curation;

import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.runtime.metrics.OperationMetricsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.neo4j.bench.ldbc.Retries;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ResultsCurator
{
    private static final Logger LOG = LoggerFactory.getLogger( ResultsCurator.class );

    private static final DecimalFormat DOUBLE_FORMAT = new DecimalFormat( "###,###,###,###,##0.00" );
    private static final DecimalFormat INTEGER_FORMAT = new DecimalFormat( "###,###,###,###,##0" );
    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
    private static final FilenameFilter RESULTS_DIR_FILTER = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return dir.isDirectory() &&
                   (
                           name.matches( "results_\\d\\d-cores-\\d\\d-sockets" ) ||
                           name.matches( "results_\\d\\d\\d-cores-\\d\\d-sockets" )
                   );
        }

        @Override
        public String toString()
        {
            return "results_\\d\\d[\\d]-cores-\\d\\d-sockets";
        }
    };
    private static final FilenameFilter NON_WARMUP_SUMMARY_RESULT_JSON_FILTER = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return !name.contains( "WARMUP" ) && name.endsWith( "-results.json" );
        }

        @Override
        public String toString()
        {
            return "-results.json";
        }
    };
    private static final FilenameFilter SUMMARY_RESULT_JSON_FILTER = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return name.endsWith( "-results.json" );
        }

        @Override
        public String toString()
        {
            return "-results.json";
        }
    };
    private static final FilenameFilter RESULTS_LOG_CSV_FILTER = new FilenameFilter()
    {
        @Override
        public boolean accept( File dir, String name )
        {
            return name.endsWith( "-results_log.csv" );
        }

        @Override
        public String toString()
        {
            return "-results_log.csv";
        }
    };

    public static void main( String[] args ) throws IOException
    {
        File resultsDir = getDirectoryOrExit( args );
        LOG.debug( format( "Curating results for: %s", resultsDir.getAbsolutePath() ) );

        // TODO make available via CLI
        calculateStatsForCoreScalingTest( resultsDir );
        // TODO make available via CLI
//        calculateStatsForSingleTest(resultsDir);
    }

    private static void calculateStatsForCoreScalingTest( File resultsDir ) throws IOException
    {
        ResultsCurator resultsCurator = new ResultsCurator();

        // summary of all directories
        resultsCurator.makeSummaryThroughputVsSocketsVsCoresFile(
                resultsDir,
                RESULTS_DIR_FILTER,
                NON_WARMUP_SUMMARY_RESULT_JSON_FILTER);
        resultsCurator.makeLatencyPerOperationTypeVsSocketsVsCoresFiles(
                resultsDir,
                RESULTS_DIR_FILTER,
                NON_WARMUP_SUMMARY_RESULT_JSON_FILTER);
        // per directory
        resultsCurator.createCsvSummariesFromJsonResultsFiles(
                resultsDir,
                RESULTS_DIR_FILTER,
                SUMMARY_RESULT_JSON_FILTER);
        long bucketDurationAsMilli = TimeUnit.SECONDS.toMillis( 5 );
        resultsCurator.createCsvSummariesFromCsvResultsLogFiles(
                resultsDir,
                RESULTS_DIR_FILTER,
                RESULTS_LOG_CSV_FILTER,
                bucketDurationAsMilli);
    }

    // ================================================================================
    //           ALL RESULTS LOG CSV FILES --> SUMMARY LATENCY CSV
    // ================================================================================

    private void makeLatencyPerOperationTypeVsSocketsVsCoresFiles(
            File parentResultsDir,
            FilenameFilter resultDirectoriesFilter,
            FilenameFilter resultJsonFileFilter ) throws IOException
    {
        Map<Integer,Map<Integer,Map<String,LatenciesAtCoresAtSockets>>> latenciesAtCoresAtSocketsMap = new HashMap<>();

        for ( File resultsDir : parentResultsDir.listFiles( resultDirectoriesFilter ) )
        {
            int sockets = extractSockets( resultsDir.getName() );
            int cores = extractCores( resultsDir.getName() );

            Map<Integer,Map<String,LatenciesAtCoresAtSockets>> latenciesAtCoresAtSocketMap =
                    latenciesAtCoresAtSocketsMap.get( sockets );
            if ( null == latenciesAtCoresAtSocketMap )
            {
                latenciesAtCoresAtSocketMap = new HashMap<>();
                latenciesAtCoresAtSocketsMap.put( sockets, latenciesAtCoresAtSocketMap );
            }

            File inputResultsJsonFile = getSingleFileFromDirectory( resultsDir, resultJsonFileFilter );
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson( inputResultsJsonFile );

            Map<String,LatenciesAtCoresAtSockets> latenciesForOperations = latenciesAtCoresAtSocketMap.get( cores );
            if ( null != latenciesForOperations )
            {
                throw new RuntimeException(
                        format( "Duplicate entries for %s sockets %s cores", sockets, cores ) );
            }
            latenciesForOperations = new HashMap<>();
            latenciesAtCoresAtSocketMap.put( cores, latenciesForOperations );

            for ( OperationMetricsSnapshot operationMetricsSnapshots : resultsSnapshot.allMetrics() )
            {
                ContinuousMetricSnapshot continuousMetricSnapshot = operationMetricsSnapshots.runTimeMetric();
                LatenciesAtCoresAtSockets latenciesAtCoresAtSockets = new LatenciesAtCoresAtSockets(
                        operationMetricsSnapshots.name(),
                        sockets,
                        cores,
                        continuousMetricSnapshot.count(),
                        continuousMetricSnapshot.mean(),
                        continuousMetricSnapshot.percentile50(),
                        continuousMetricSnapshot.percentile90(),
                        continuousMetricSnapshot.percentile95(),
                        continuousMetricSnapshot.percentile99()
                );
                latenciesForOperations.put( operationMetricsSnapshots.name(), latenciesAtCoresAtSockets );
            }
        }

//      ----------------------------------
//      THROUGHPUT VS SOCKETS VS CORES
//      ----------------------------------

        List<LatenciesAtCoresAtSockets> latenciesAtCoresAtSocketsList = new ArrayList<>();
        for ( Map.Entry<Integer,Map<Integer,Map<String,LatenciesAtCoresAtSockets>>> latenciesAtSockets :
                latenciesAtCoresAtSocketsMap.entrySet() )
        {
            for ( Map.Entry<Integer,Map<String,LatenciesAtCoresAtSockets>> latenciesAtCores : latenciesAtSockets
                    .getValue().entrySet() )
            {
                Collection<LatenciesAtCoresAtSockets> latencies = latenciesAtCores.getValue().values();
                for ( LatenciesAtCoresAtSockets latenciesAtCoresAtSockets : latencies )
                {
                    latenciesAtCoresAtSocketsList.add( latenciesAtCoresAtSockets );
                }
            }
        }

        Collections.sort( latenciesAtCoresAtSocketsList );

        File latenciesCsv = new File( parentResultsDir, "latencies_vs_sockets_vs_cores.csv" );
        FileUtils.deleteQuietly( latenciesCsv );
        String[] allLatenciesHeaders = new String[]{
                "sockets",
                "cores",
                "name",
                "count",
                "total",
                "mean",
                "50th",
                "90th",
                "95th",
                "99th"
        };
        SimpleCsvFileWriter allLatenciesCsvFileWriter =
                new SimpleCsvFileWriter( latenciesCsv, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );
        allLatenciesCsvFileWriter.writeRow( allLatenciesHeaders );

        for ( LatenciesAtCoresAtSockets latenciesAtCoresAtSockets : latenciesAtCoresAtSocketsList )
        {
            String[] row = new String[]{
                    INTEGER_FORMAT.format( latenciesAtCoresAtSockets.sockets ),
                    INTEGER_FORMAT.format( latenciesAtCoresAtSockets.cores ),
                    latenciesAtCoresAtSockets.name,
                    INTEGER_FORMAT.format( latenciesAtCoresAtSockets.count ),
                    INTEGER_FORMAT.format( latenciesAtCoresAtSockets.count * latenciesAtCoresAtSockets.mean ),
                    DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.mean ),
                    DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.percentile50 ),
                    DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.percentile90 ),
                    DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.percentile95 ),
                    DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.percentile99 ),
                    };
            allLatenciesCsvFileWriter.writeRow( row );
        }
        allLatenciesCsvFileWriter.close();

//      ----------------------------------
//      MEAN VS SOCKETS VS CORES
//      ----------------------------------

        File meanCsv = new File( parentResultsDir, "mean_vs_sockets_vs_cores.csv" );
        FileUtils.deleteQuietly( meanCsv );

        Set<String> operationTypes = new HashSet<>();
        for ( LatenciesAtCoresAtSockets latenciesAtCoresAtSockets : latenciesAtCoresAtSocketsList )
        {
            operationTypes.add( latenciesAtCoresAtSockets.name );
        }
        String[] operationTypesArray = operationTypes.toArray( new String[0] );
        Arrays.sort( operationTypesArray );
        int rowLength = 2 + operationTypesArray.length;
        String[] row = new String[rowLength];
        row[0] = "sockets";
        row[1] = "cores";
        System.arraycopy( operationTypesArray, 0, row, 2, operationTypesArray.length );

        SimpleCsvFileWriter oneLatencyCsvFileWriter =
                new SimpleCsvFileWriter( meanCsv, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );

        int index = 0;
        for ( LatenciesAtCoresAtSockets latenciesAtCoresAtSockets : latenciesAtCoresAtSocketsList )
        {
            switch ( index )
            {
            case 0:
            {
                oneLatencyCsvFileWriter.writeRow( row );
                row = new String[row.length];
                row[0] = INTEGER_FORMAT.format( latenciesAtCoresAtSockets.sockets );
                row[1] = INTEGER_FORMAT.format( latenciesAtCoresAtSockets.cores );
                row[2] = DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.mean );
                index = 3 % row.length;
                break;
            }
            case 1:
            {
                row[index] = INTEGER_FORMAT.format( latenciesAtCoresAtSockets.cores );
                index = (index + 1) % row.length;
                break;
            }
            default:
            {
                row[index] = DOUBLE_FORMAT.format( latenciesAtCoresAtSockets.mean );
                index = (index + 1) % row.length;
                break;
            }
            }
        }
        oneLatencyCsvFileWriter.writeRow( row );
        oneLatencyCsvFileWriter.close();
    }

    // ================================================================================
    //           ALL RESULTS LOG CSV FILES --> SUMMARY THROUGHPUT CSV
    // ================================================================================

    private void makeSummaryThroughputVsSocketsVsCoresFile(
            File parentResultsDir,
            FilenameFilter resultDirectoriesFilter,
            FilenameFilter resultJsonFileFilter ) throws IOException
    {
        String[] headers = new String[]{
                "sockets",
                "cores",
                "throughput"
        };

        Map<Integer,Map<Integer,ThroughputAtCoresAtSockets>> throughputAtCoresAtSocketsMap = new HashMap<>();

        for ( File resultsDir : parentResultsDir.listFiles( resultDirectoriesFilter ) )
        {
            int sockets = extractSockets( resultsDir.getName() );
            int cores = extractCores( resultsDir.getName() );

            Map<Integer,ThroughputAtCoresAtSockets> throughputAtCoresAtSocketMap =
                    throughputAtCoresAtSocketsMap.get( sockets );
            if ( null == throughputAtCoresAtSocketMap )
            {
                throughputAtCoresAtSocketMap = new HashMap<>();
                throughputAtCoresAtSocketsMap.put( sockets, throughputAtCoresAtSocketMap );
            }

            File inputResultsJsonFile = getSingleFileFromDirectory( resultsDir, resultJsonFileFilter );
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson( inputResultsJsonFile );

            ThroughputAtCoresAtSockets throughputAtCoresAtSockets = throughputAtCoresAtSocketMap.get( cores );
            if ( null != throughputAtCoresAtSockets )
            {
                throw new RuntimeException(
                        format( "Duplicate entries for %s sockets %s cores", sockets, cores ) );
            }
            double throughput =
                    (resultsSnapshot.totalOperationCount() / (double) resultsSnapshot.totalRunDurationAsNano()) *
                    1_000_000_000;
            throughputAtCoresAtSockets = new ThroughputAtCoresAtSockets( sockets, cores, throughput );
            throughputAtCoresAtSocketMap.put( cores, throughputAtCoresAtSockets );
        }

        List<ThroughputAtCoresAtSockets> throughputAtCoresAtSocketsList = new ArrayList<>();
        for ( Map.Entry<Integer,Map<Integer,ThroughputAtCoresAtSockets>> throughputAtSockets :
                throughputAtCoresAtSocketsMap
                        .entrySet() )
        {
            for ( Map.Entry<Integer,ThroughputAtCoresAtSockets> throughputAtCores : throughputAtSockets.getValue()
                                                                                                       .entrySet() )
            {
                throughputAtCoresAtSocketsList.add(
                        throughputAtCores.getValue()
                                                  );
            }
        }

        Collections.sort( throughputAtCoresAtSocketsList );

        File throughputCsv = new File( parentResultsDir, "throughput_vs_sockets_vs_cores.csv" );
        FileUtils.deleteQuietly( throughputCsv );
        SimpleCsvFileWriter csvFileWriter =
                new SimpleCsvFileWriter( throughputCsv, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );
        csvFileWriter.writeRow( headers );

        for ( ThroughputAtCoresAtSockets throughputAtCoresAtSockets : throughputAtCoresAtSocketsList )
        {
            String[] row = new String[]{
                    INTEGER_FORMAT.format( throughputAtCoresAtSockets.sockets ),
                    INTEGER_FORMAT.format( throughputAtCoresAtSockets.cores ),
                    DOUBLE_FORMAT.format( throughputAtCoresAtSockets.throughput )
            };
            csvFileWriter.writeRow( row );
        }

        csvFileWriter.close();
    }

    // ================================================================================
    //           SUMMARY JSON --> SUMMARY CSV
    // ================================================================================

    private void createCsvSummariesFromJsonResultsFiles(
            File parentResultsDir,
            FilenameFilter resultDirectoriesFilter,
            FilenameFilter resultJsonFileFilter ) throws IOException
    {
        for ( File resultsDir : parentResultsDir.listFiles( resultDirectoriesFilter ) )
        {
            String resultsCsvFilePath =
                    format( "cores-%s-sockets-%s-summary_results.csv",
                            extractCores( resultsDir.getName() ),
                            extractSockets( resultsDir.getName() ) );
            File outputResultsCsvFile = new File( resultsDir, resultsCsvFilePath );
            for ( File inputResultsJsonFile : getFilesFromDirectory( resultsDir, resultJsonFileFilter ) )
            {
                createCsvSummariesFromJsonResultsFile( outputResultsCsvFile, inputResultsJsonFile );
            }
        }
    }

    private void createCsvSummariesFromJsonResultsFile(
            File outputResultsCsvFile,
            File inputResultsJsonFile ) throws IOException
    {
        String[] headers = new String[]{
                "name",
                "count",
                "min",
                "max",
                "mean",
                "50th",
                "90th",
                "95th",
                "99th"
        };

        FileUtils.deleteQuietly( outputResultsCsvFile );
        SimpleCsvFileWriter csvFileWriter =
                new SimpleCsvFileWriter( outputResultsCsvFile, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );
        csvFileWriter.writeRow( headers );

        WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson( inputResultsJsonFile );

        for ( OperationMetricsSnapshot metricsSnapshot : resultsSnapshot.allMetrics() )
        {
            ContinuousMetricSnapshot runTimeMetricSnapshot = metricsSnapshot.runTimeMetric();
            String[] row = new String[]{
                    metricsSnapshot.name(),
                    Long.toString( metricsSnapshot.count() ),
                    Long.toString( runTimeMetricSnapshot.min() ),
                    Long.toString( runTimeMetricSnapshot.max() ),
                    Double.toString( runTimeMetricSnapshot.mean() ),
                    Long.toString( runTimeMetricSnapshot.percentile50() ),
                    Long.toString( runTimeMetricSnapshot.percentile90() ),
                    Long.toString( runTimeMetricSnapshot.percentile95() ),
                    Long.toString( runTimeMetricSnapshot.percentile99() )
            };
            csvFileWriter.writeRow( row );
        }

        csvFileWriter.close();
    }

    // ================================================================================
    //           FULL RESULTS LOG CSV --> VARIOUS SUMMARIES
    // ================================================================================

    private void createCsvSummariesFromCsvResultsLogFiles(
            File parentResultsDir,
            FilenameFilter resultDirectoriesFilter,
            FilenameFilter resultsLogCsvFileFilter,
            long bucketDurationAsMilli ) throws IOException
    {
        for ( File resultsDir : parentResultsDir.listFiles( resultDirectoriesFilter ) )
        {
            for ( File inputResultsLogCsvFile : getFilesFromDirectory( resultsDir, resultsLogCsvFileFilter ) )
            {
                createCsvSummariesFromCsvResultsLogFile( inputResultsLogCsvFile, bucketDurationAsMilli );
            }
        }
    }

    private void createCsvSummariesFromCsvResultsLogFile(
            File inputResultsLogCsvFile,
            long bucketDurationAsMilli ) throws IOException
    {
        String inputResultsLogCsvFilename = nameWithoutExtension( inputResultsLogCsvFile );

        String outputWindowedResultsCsvFilePath =
                format( "%s---windowed_results_log.csv", inputResultsLogCsvFilename );
        File outputResultsCsvFile =
                new File( inputResultsLogCsvFile.getParentFile(), outputWindowedResultsCsvFilePath );
        createSummaryWindowedResultsLogOfFullResultsLogFile(
                bucketDurationAsMilli,
                outputResultsCsvFile,
                inputResultsLogCsvFile );

        String resultCodeSummaryCsvFilePath =
                format( "%s---result_code_summary.csv", inputResultsLogCsvFilename );
        File outputResultCodeSummaryCsvFile =
                new File( inputResultsLogCsvFile.getParentFile(), resultCodeSummaryCsvFilePath );
        createSummaryResultCodeFile( outputResultCodeSummaryCsvFile, inputResultsLogCsvFile );

        String isolationRetriesCsvFilePath =
                format( "%s---isolation_retries.csv", inputResultsLogCsvFilename );
        File outputIsolationRetriesCsvFile =
                new File( inputResultsLogCsvFile.getParentFile(), isolationRetriesCsvFilePath );
        createIsolationRetriesFile( outputIsolationRetriesCsvFile, inputResultsLogCsvFile );

        String retryableRetriesCsvFilePath =
                format( "%s---retryable_retries.csv", inputResultsLogCsvFilename );
        File outputRetryableRetriesCsvFile =
                new File( inputResultsLogCsvFile.getParentFile(), retryableRetriesCsvFilePath );
        createRetryableRetriesFile( outputRetryableRetriesCsvFile, inputResultsLogCsvFile );

        String deadlockRetriesCsvFilePath =
                format( "%s---deadlock_retries.csv", inputResultsLogCsvFilename );
        File outputDeadlockRetriesCsvFile =
                new File( inputResultsLogCsvFile.getParentFile(), deadlockRetriesCsvFilePath );
        createDeadlockRetriesFile( outputDeadlockRetriesCsvFile, inputResultsLogCsvFile );
    }

    // ================================================================================
    //           FULL RESULTS LOG CSV --> WINDOWED RESULTS LOG CSV
    // ================================================================================

    private void createSummaryWindowedResultsLogOfFullResultsLogFile(
            long bucketDurationAsMilli,
            File outputWindowedResultsCsvFilePath,
            File inputResultsLogCsvFile ) throws IOException
    {
        FileUtils.deleteQuietly( outputWindowedResultsCsvFilePath );
        long minimumStartTimeAsMilli = Long.MAX_VALUE;
        long maximumStartTimeAsMilli = Long.MIN_VALUE;
        // operation_type|
        // scheduled_start_time_MILLISECONDS|
        // actual_start_time_MILLISECONDS|
        // execution_duration_MILLISECONDS|
        // result_code
        SimpleCsvFileReader inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        );
        // skip headers
        inputResultsLogCsvReader.next();
        while ( inputResultsLogCsvReader.hasNext() )
        {
            String[] row = inputResultsLogCsvReader.next();
            long startTimeAsMilli = Long.parseLong( row[2] );
            if ( startTimeAsMilli < minimumStartTimeAsMilli )
            {
                minimumStartTimeAsMilli = startTimeAsMilli;
            }
            if ( startTimeAsMilli > maximumStartTimeAsMilli )
            {
                maximumStartTimeAsMilli = startTimeAsMilli;
            }
        }
        inputResultsLogCsvReader.close();

        long resultsLogDurationAsMilli = maximumStartTimeAsMilli - minimumStartTimeAsMilli;
        int bucketCount =
                (int) Math.round( Math.ceil( resultsLogDurationAsMilli / (double) bucketDurationAsMilli ) );
        ThroughputWindow[] throughputWindows = new ThroughputWindow[bucketCount];
        for ( int i = 0; i < bucketCount; i++ )
        {
            long bucketStartTimeInclusiveAsMilli = minimumStartTimeAsMilli + (i * bucketDurationAsMilli);
            long bucketEndTimeExclusiveAsMilli = bucketStartTimeInclusiveAsMilli + bucketDurationAsMilli;
            ThroughputWindow throughputWindow =
                    new ThroughputWindow( bucketStartTimeInclusiveAsMilli, bucketEndTimeExclusiveAsMilli );
            throughputWindows[i] = throughputWindow;
        }

        inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
        // skip headers
        inputResultsLogCsvReader.next();
        while ( inputResultsLogCsvReader.hasNext() )
        {
            String[] row = inputResultsLogCsvReader.next();
            long startTime = Long.parseLong( row[2] );
            int bucketIndex = -1;
            for ( int i = 0; i < bucketCount; i++ )
            {
                ThroughputWindow throughputWindow = throughputWindows[i];
                if ( throughputWindow.startTimeInclusiveAsMilli <= startTime &&
                     throughputWindow.endTimeExclusiveAsMilli > startTime )
                {
                    bucketIndex = i;
                    break;
                }
            }
            throughputWindows[bucketIndex].count++;
        }
        inputResultsLogCsvReader.close();

        String[] headers = new String[]{
                "window_start_formatted",
                "window_start",
                "window_end",
                "count",
                "duration",
                "throughput"
        };
        SimpleCsvFileWriter csvFileWriter =
                new SimpleCsvFileWriter( outputWindowedResultsCsvFilePath,
                                         SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR );
        csvFileWriter.writeRow( headers );
        String[] row = new String[headers.length];
        long minStartTimeInclusiveAsMilli = Long.MAX_VALUE;
        for ( int i = 0; i < bucketCount; i++ )
        {
            minStartTimeInclusiveAsMilli =
                    Math.min( minStartTimeInclusiveAsMilli, throughputWindows[i].startTimeInclusiveAsMilli );
        }
        for ( int i = 0; i < bucketCount; i++ )
        {
            row[0] = DATE_TIME_FORMAT.format( new Date( throughputWindows[i].startTimeInclusiveAsMilli ) );
            row[1] = Long.toString( throughputWindows[i].startTimeInclusiveAsMilli );
            row[2] = Long.toString( throughputWindows[i].endTimeExclusiveAsMilli );
            row[3] = Integer.toString( throughputWindows[i].count );
            row[4] = Long.toString( throughputWindows[i].startTimeInclusiveAsMilli - minStartTimeInclusiveAsMilli );
            double operationsPerMilli = (double) throughputWindows[i].count / throughputWindows[i].durationAsMilli;
            double operationsPerSecond = operationsPerMilli * 1000d;
            row[5] = DOUBLE_FORMAT.format( operationsPerSecond );
            csvFileWriter.writeRow( row );
        }
        csvFileWriter.close();
    }

// ================================================================================
    //           FULL RESULTS LOG CSV --> SUMMARY RESULT CODE CSV
    // ================================================================================

    private void createSummaryResultCodeFile(
            File outputResultCodeSummaryCsvFile,
            File inputResultsLogCsvFile ) throws IOException
    {
        FileUtils.deleteQuietly( outputResultCodeSummaryCsvFile );

        Map<String,ResultCodeSummary> summaries = new HashMap<>();

        // 0 operation_type|
        // 1 scheduled_start_time_MILLISECONDS|
        // 2 actual_start_time_MILLISECONDS|
        // 3 execution_duration_MILLISECONDS|
        // 4 result_code
        try ( SimpleCsvFileReader inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        ) )
        {
            // skip headers
            inputResultsLogCsvReader.next();
            while ( inputResultsLogCsvReader.hasNext() )
            {
                String[] row = inputResultsLogCsvReader.next();
                String operationType = row[0];
                ResultCodeSummary summary = summaries.get( operationType );
                if ( null == summary )
                {
                    summary = new ResultCodeSummary( operationType );
                    summaries.put( operationType, summary );
                }
                int resultCode = Integer.parseInt( row[4] );
                summary.record( resultCode );
            }
        }

        String[] headers = new String[]{
                "operation",
                "count",
                "result_code_sum",
                "result_code_mean",
                "result_code_min",
                "result_code_max"
        };

        try ( SimpleCsvFileWriter csvFileWriter = new SimpleCsvFileWriter(
                outputResultCodeSummaryCsvFile,
                SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR
        ) )
        {
            csvFileWriter.writeRow( headers );
            String[] row = new String[headers.length];

            for ( ResultCodeSummary summary : summaries.values() )
            {
                row[0] = summary.operationType();
                row[1] = Long.toString( summary.count() );
                row[2] = Long.toString( summary.sum() );
                row[3] = Long.toString( summary.mean() );
                row[4] = Long.toString( summary.min() );
                row[5] = Long.toString( summary.max() );
                csvFileWriter.writeRow( row );
            }
        }
    }

    // ================================================================================
    //           FULL RESULTS LOG CSV --> SUMMARY ISOLATION RETRIES CSV
    // ================================================================================

    private void createIsolationRetriesFile(
            File outputIsolationRetriesCsvFile,
            File inputResultsLogCsvFile ) throws IOException
    {
        FileUtils.deleteQuietly( outputIsolationRetriesCsvFile );
        Map<String,ResultCodeSummary> summaries = new HashMap<>();

        // 0 operation_type|
        // 1 scheduled_start_time_MILLISECONDS|
        // 2 actual_start_time_MILLISECONDS|
        // 3 execution_duration_MILLISECONDS|
        // 4 result_code
        try ( SimpleCsvFileReader inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        ) )
        {
            // skip headers
            inputResultsLogCsvReader.next();
            while ( inputResultsLogCsvReader.hasNext() )
            {
                String[] row = inputResultsLogCsvReader.next();
                String operationType = row[0];
                ResultCodeSummary summary = summaries.get( operationType );
                if ( null == summary )
                {
                    summary = new ResultCodeSummary( operationType );
                    summaries.put( operationType, summary );
                }
                int isolationRetries = Retries.decodeResultCodeToIsolationRetryCount( Integer.parseInt( row[4] ) );
                summary.record( isolationRetries );
            }
        }

        String[] headers = new String[]{
                "operation",
                "count",
                "isolation_retries_sum",
                "isolation_retries_mean",
                "isolation_retries_min",
                "isolation_retries_max"
        };

        try ( SimpleCsvFileWriter csvFileWriter = new SimpleCsvFileWriter(
                outputIsolationRetriesCsvFile,
                SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR
        ) )
        {
            csvFileWriter.writeRow( headers );
            String[] row = new String[headers.length];

            for ( ResultCodeSummary summary : summaries.values() )
            {
                row[0] = summary.operationType();
                row[1] = Long.toString( summary.count() );
                row[2] = Long.toString( summary.sum() );
                row[3] = Long.toString( summary.mean() );
                row[4] = Long.toString( summary.min() );
                row[5] = Long.toString( summary.max() );
                csvFileWriter.writeRow( row );
            }
        }
    }

    // ================================================================================
    //           FULL RESULTS LOG CSV --> SUMMARY ARBITRARY RETRIES CSV
    // ================================================================================

    private void createRetryableRetriesFile(
            File outputRetryableRetriesCsvFile,
            File inputResultsLogCsvFile ) throws IOException
    {
        FileUtils.deleteQuietly( outputRetryableRetriesCsvFile );
        Map<String,ResultCodeSummary> summaries = new HashMap<>();

        // 0 operation_type|
        // 1 scheduled_start_time_MILLISECONDS|
        // 2 actual_start_time_MILLISECONDS|
        // 3 execution_duration_MILLISECONDS|
        // 4 result_code
        try ( SimpleCsvFileReader inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        ) )
        {
            // skip headers
            inputResultsLogCsvReader.next();
            while ( inputResultsLogCsvReader.hasNext() )
            {
                String[] row = inputResultsLogCsvReader.next();
                String operationType = row[0];
                ResultCodeSummary summary = summaries.get( operationType );
                if ( null == summary )
                {
                    summary = new ResultCodeSummary( operationType );
                    summaries.put( operationType, summary );
                }
                int retryableRetries = Retries.decodeResultCodeToRetryableRetryCount( Integer.parseInt( row[4] ) );
                summary.record( retryableRetries );
            }
        }

        String[] headers = new String[]{
                "operation",
                "count",
                "retryable_retries_sum",
                "retryable_retries_mean",
                "retryable_retries_min",
                "retryable_retries_max"
        };

        try ( SimpleCsvFileWriter csvFileWriter = new SimpleCsvFileWriter(
                outputRetryableRetriesCsvFile,
                SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR
        ) )
        {
            csvFileWriter.writeRow( headers );
            String[] row = new String[headers.length];

            for ( ResultCodeSummary summary : summaries.values() )
            {
                row[0] = summary.operationType();
                row[1] = Long.toString( summary.count() );
                row[2] = Long.toString( summary.sum() );
                row[3] = Long.toString( summary.mean() );
                row[4] = Long.toString( summary.min() );
                row[5] = Long.toString( summary.max() );
                csvFileWriter.writeRow( row );
            }
        }
    }

    // ================================================================================
    //           FULL RESULTS LOG CSV --> SUMMARY DEADLOCK RETRIES CSV
    // ================================================================================

    private void createDeadlockRetriesFile(
            File outputDeadlockRetriesCsvFile,
            File inputResultsLogCsvFile ) throws IOException
    {
        FileUtils.deleteQuietly( outputDeadlockRetriesCsvFile );

        Map<String,ResultCodeSummary> summaries = new HashMap<>();

        // 0 operation_type|
        // 1 scheduled_start_time_MILLISECONDS|
        // 2 actual_start_time_MILLISECONDS|
        // 3 execution_duration_MILLISECONDS|
        // 4 result_code
        try ( SimpleCsvFileReader inputResultsLogCsvReader = new SimpleCsvFileReader(
                inputResultsLogCsvFile,
                SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING
        ) )
        {
            // skip headers
            inputResultsLogCsvReader.next();
            while ( inputResultsLogCsvReader.hasNext() )
            {
                String[] row = inputResultsLogCsvReader.next();
                String operationType = row[0];
                ResultCodeSummary summary = summaries.get( operationType );
                if ( null == summary )
                {
                    summary = new ResultCodeSummary( operationType );
                    summaries.put( operationType, summary );
                }
                int deadlockRetries = Retries.decodeResultCodeToDeadlockRetryCount( Integer.parseInt( row[4] ) );
                summary.record( deadlockRetries );
            }
        }

        String[] headers = new String[]{
                "operation",
                "count",
                "deadlock_retries_sum",
                "deadlock_retries_mean",
                "deadlock_retries_min",
                "deadlock_retries_max"
        };

        try ( SimpleCsvFileWriter csvFileWriter = new SimpleCsvFileWriter(
                outputDeadlockRetriesCsvFile,
                SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR
        ) )
        {
            csvFileWriter.writeRow( headers );
            String[] row = new String[headers.length];

            for ( ResultCodeSummary summary : summaries.values() )
            {
                row[0] = summary.operationType();
                row[1] = Long.toString( summary.count() );
                row[2] = Long.toString( summary.sum() );
                row[3] = Long.toString( summary.mean() );
                row[4] = Long.toString( summary.min() );
                row[5] = Long.toString( summary.max() );
                csvFileWriter.writeRow( row );
            }
        }
    }

    // ================================================================================
    //           Utils
    // ================================================================================

    private static File getDirectoryOrExit( String... args )
    {
        if ( args.length != 1 )
        {
            LOG.debug( format( "Expected 1 parameter (path), found %s: %s",
                                        args.length,
                                        Arrays.toString( args )
                                      ) );
            System.exit( -1 );
        }
        File parentResultsDir = new File( args[0] );
        if ( !parentResultsDir.exists() )
        {
            LOG.debug( format( "Directory does not exist: %s", parentResultsDir.getAbsolutePath() ) );
            System.exit( -1 );
        }
        if ( !parentResultsDir.isDirectory() )
        {
            LOG.debug( format( "Not a directory: %s", parentResultsDir.getAbsolutePath() ) );
            System.exit( -1 );
        }
        return parentResultsDir;
    }

    private File getSingleFileFromDirectory( File directory, FilenameFilter fileFilter )
    {
        List<File> files = getFilesFromDirectory( directory, fileFilter );
        if ( files.size() != 1 )
        {
            throw new RuntimeException(
                    format( "Expected to find 1 file matching %s in %s but found %s\n%s",
                            fileFilter.toString(),
                            directory.getAbsolutePath(),
                            files.size(),
                            files.toString()
                          ) );
        }
        return files.get( 0 );
    }

    private List<File> getFilesFromDirectory( File directory, FilenameFilter fileFilter )
    {
        List<File> files = new ArrayList<>();
        Collections.addAll( files, directory.listFiles( fileFilter ) );
        return files;
    }

    private int extractCores( String filename )
    {
        int startIndex = "results_".length();
        String coresSubstring = "-cores-";
        int endIndex = filename.indexOf( coresSubstring );
        return Integer.parseInt( filename.substring( startIndex, endIndex ) );
    }

    private int extractSockets( String filename )
    {
        String coresSubstring = "-cores-";
        int startIndex = filename.indexOf( coresSubstring ) + coresSubstring.length();
        String socketsSubstring = "-sockets";
        int endIndex = filename.indexOf( socketsSubstring );
        return Integer.parseInt( filename.substring( startIndex, endIndex ) );
    }

    private String nameWithoutExtension( File file )
    {
        return (!file.getName().contains( "." ))
               ? file.getName()
               : file.getName().substring( 0, file.getName().lastIndexOf( '.' ) );
    }

    private static class ResultCodeSummary
    {
        private final String operationType;
        private long min = Integer.MAX_VALUE;
        private long max = Integer.MIN_VALUE;
        private long count;
        private long sum;

        ResultCodeSummary( String operationType )
        {
            this.operationType = operationType;
        }

        private void record( int resultCode )
        {
            count++;
            sum += resultCode;
            if ( resultCode < min )
            {
                min = resultCode;
            }
            if ( resultCode > max )
            {
                max = resultCode;
            }
        }

        private String operationType()
        {
            return operationType;
        }

        private long count()
        {
            return count;
        }

        private long min()
        {
            return min;
        }

        private long max()
        {
            return max;
        }

        private long sum()
        {
            return sum;
        }

        private long mean()
        {
            return sum / count;
        }
    }
}
