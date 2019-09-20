/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.queries.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.AttachTestRunAnnotation;
import com.neo4j.bench.client.queries.SubmitTestRun;
import com.neo4j.bench.common.model.Annotation;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.Benchmark.Mode;
import com.neo4j.bench.common.model.BenchmarkConfig;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.model.BenchmarkPlan;
import com.neo4j.bench.common.model.BenchmarkTool;
import com.neo4j.bench.common.model.BranchAndVersion;
import com.neo4j.bench.common.model.Environment;
import com.neo4j.bench.common.model.Java;
import com.neo4j.bench.common.model.Metrics;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.model.Project;
import com.neo4j.bench.common.model.Repository;
import com.neo4j.bench.common.model.TestRun;
import com.neo4j.bench.common.model.TestRunError;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.RichRandom;

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.neo4j.bench.common.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.common.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.common.model.Repository.MACRO_BENCH;
import static com.neo4j.bench.common.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SyntheticStoreGenerator
{
    private static final RichRandom RNG = new RichRandom( 42 );
    private static final double TEST_RUN_ANNOTATION_PROBABILITY = 0.5;
    private static final double METRICS_ANNOTATION_PROBABILITY = 0.5;
    private static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat( "#,###,##0.00" );
    private static final int DEFAULT_DAYS = 7;
    private static final int DEFAULT_RESULTS_PER_DAY = 10;
    private static final Map<String,String> BENCHMARK_PARAMETERS =
            IntStream.range( 0, 10 ).mapToObj( Integer::toString ).collect( toMap( s -> "k_" + s, s -> "v_" + s ) );
    private static final Group[] DEFAULT_BENCHMARK_GROUPS = IntStream.range( 0, 4 )
                                                                     .mapToObj( Integer::toString )
                                                                     .map( name -> Group.from( name, 10 ) )
                                                                     .toArray( Group[]::new );
    private static final String[] DEFAULT_NEO4J_VERSIONS = {"3.0.2", "3.0.1", "3.0.0"};
    private static final Edition[] DEFAULT_NEO4J_EDITIONS = Edition.values();
    private static final int DEFAULT_SETTINGS_IN_CONFIG = 50;
    private static final Repository[] TOOLS = {MICRO_BENCH, MACRO_BENCH, LDBC_BENCH, IMPORT_BENCH};
    private static final Repository[] PROJECTS = {NEO4J};
    private static final String[] DEFAULT_OPERATING_SYSTEMS = {"Windows", "OSX", "Ubuntu"};
    private static final String[] DEFAULT_SERVERS = {"Skalleper", "local", "AWS", "Mattis", "Borka"};
    private static final String[] DEFAULT_JVM_ARGS = {"-XX:+UseG1GC -Xmx4g", "-server", "-Xmx12g"};
    private static final String[] DEFAULT_JVMS = {"Oracle", "OpenJDK"};
    private static final String[] DEFAULT_JVM_VERSIONS = {"1.80_66", "1.80_12", "1.7.0_42"};
    private static final String[] DEFAULT_NEO4J_BRANCH_OWNERS = {NEO4J.defaultOwner()};
    private static final String[] DEFAULT_TOOL_BRANCH_OWNERS = {MICRO_BENCH.defaultOwner(), LDBC_BENCH.defaultOwner()};
    private static final TimeUnit[] UNITS = new TimeUnit[]{SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS};

    private static final Supplier<TimeUnit> UNIT = () -> UNITS[RNG.nextInt( 0, UNITS.length - 1 )];
    private static final Supplier<Double> MIN_NS = () -> RNG.nextDouble( 1, 100 );
    private static final Supplier<Double> MEAN_NS = () -> RNG.nextDouble( 1_000_000, 1_000_000_000 );
    private static final Supplier<Double> ERROR_NS = () -> RNG.nextDouble( 5, 50 );
    private static final Supplier<Double> ERROR_CONFIDENCE = () -> RNG.nextDouble( 0, 1 );
    private static final Supplier<Double> PERC_25_NS = () -> RNG.nextDouble( 150, 350 );
    private static final Supplier<Double> PERC_50_NS = () -> RNG.nextDouble( 400, 600 );
    private static final Supplier<Double> PERC_75_NS = () -> RNG.nextDouble( 700, 800 );
    private static final Supplier<Double> PERC_90_NS = () -> RNG.nextDouble( 800, 900 );
    private static final Supplier<Double> PERC_95_NS = () -> RNG.nextDouble( 900, 950 );
    private static final Supplier<Double> PERC_99_NS = () -> RNG.nextDouble( 950, 970 );
    private static final Supplier<Double> PERC_99_9_NS = () -> RNG.nextDouble( 970, 1000 );
    private static final Supplier<Double> MAX_NS = () -> RNG.nextDouble( 100, 1050 );
    private static final Supplier<Integer> DURATION_MS = () -> RNG.nextInt( 1000, 4999 );
    private static final Supplier<Integer> BUILD = new Supplier<Integer>()
    {
        private int build;

        @Override
        public Integer get()
        {
            return build++;
        }
    };
    private static final Supplier<List<TestRunError>> ERRORS = () -> IntStream
            .range( 0, RNG.nextInt( 0, 3 ) )
            .mapToObj( i -> new TestRunError( "group", "benchmark-" + i, "Error No." + i ) )
            .collect( toList() );

    private static final long SAMPLE_SIZE = 10_000;

    public static class SyntheticStoreGeneratorBuilder
    {
        private int days = DEFAULT_DAYS;
        private int resultsPerDay = DEFAULT_RESULTS_PER_DAY;
        private Group[] benchmarkGroups = DEFAULT_BENCHMARK_GROUPS;
        private String[] neo4jVersions = DEFAULT_NEO4J_VERSIONS;
        private Edition[] neo4jEditions = DEFAULT_NEO4J_EDITIONS;
        private int settingsInConfig = DEFAULT_SETTINGS_IN_CONFIG;
        private String[] operatingSystems = DEFAULT_OPERATING_SYSTEMS;
        private String[] servers = DEFAULT_SERVERS;
        private String[] jvmArgs = DEFAULT_JVM_ARGS;
        private String[] jvms = DEFAULT_JVMS;
        private String[] jvmVersions = DEFAULT_JVM_VERSIONS;
        private String[] neo4jBranchOwners = DEFAULT_NEO4J_BRANCH_OWNERS;
        private String[] toolBranchOwners = DEFAULT_TOOL_BRANCH_OWNERS;
        private Repository[] tools = TOOLS;
        private Repository[] projects = PROJECTS;
        private boolean withPrintout;

        SyntheticStoreGeneratorBuilder withDays( int days )
        {
            this.days = days;
            return this;
        }

        SyntheticStoreGeneratorBuilder withResultsPerDay( int resultsPerDay )
        {
            this.resultsPerDay = resultsPerDay;
            return this;
        }

        SyntheticStoreGeneratorBuilder withBenchmarkGroups( Group... benchmarkGroups )
        {
            this.benchmarkGroups = benchmarkGroups;
            return this;
        }

        SyntheticStoreGeneratorBuilder withNeo4jVersions( String... neo4jVersions )
        {
            this.neo4jVersions = neo4jVersions;
            return this;
        }

        SyntheticStoreGeneratorBuilder withNeo4jEditions( Edition... neo4jEditions )
        {
            this.neo4jEditions = neo4jEditions;
            return this;
        }

        SyntheticStoreGeneratorBuilder withSettingsInConfig( int settingsInConfig )
        {
            this.settingsInConfig = settingsInConfig;
            return this;
        }

        SyntheticStoreGeneratorBuilder withOperatingSystems( String... operatingSystems )
        {
            this.operatingSystems = operatingSystems;
            return this;
        }

        SyntheticStoreGeneratorBuilder withServers( String... servers )
        {
            this.servers = servers;
            return this;
        }

        SyntheticStoreGeneratorBuilder withJvmArgs( String... jvmArgs )
        {
            this.jvmArgs = jvmArgs;
            return this;
        }

        SyntheticStoreGeneratorBuilder withJvms( String... jvms )
        {
            this.jvms = jvms;
            return this;
        }

        SyntheticStoreGeneratorBuilder withJvmVersions( String... jvmVersions )
        {
            this.jvmVersions = jvmVersions;
            return this;
        }

        SyntheticStoreGeneratorBuilder withNeo4jBranchOwners( String... neo4jBranchOwners )
        {
            this.neo4jBranchOwners = neo4jBranchOwners;
            return this;
        }

        SyntheticStoreGeneratorBuilder withToolBranchOwners( String... toolBranchOwners )
        {
            this.toolBranchOwners = toolBranchOwners;
            return this;
        }

        SyntheticStoreGeneratorBuilder withTools( Repository... tools )
        {
            this.tools = tools;
            return this;
        }

        SyntheticStoreGeneratorBuilder withProjects( Repository... projects )
        {
            this.projects = projects;
            return this;
        }

        SyntheticStoreGeneratorBuilder withPrintout( boolean withPrintout )
        {
            this.withPrintout = withPrintout;
            return this;
        }

        public SyntheticStoreGenerator build()
        {
            return new SyntheticStoreGenerator(
                    days,
                    resultsPerDay,
                    benchmarkGroups,
                    neo4jVersions,
                    neo4jEditions,
                    settingsInConfig,
                    operatingSystems,
                    servers,
                    jvmArgs,
                    jvms,
                    jvmVersions,
                    neo4jBranchOwners,
                    toolBranchOwners,
                    tools,
                    projects,
                    withPrintout
            );
        }
    }

    private final int days;

    private final int resultsPerDay;
    private final Group[] benchmarkGroups;
    private final String[] neo4jVersions;
    private final Edition[] neo4jEditions;
    private final int settingsInConfig;
    private final String[] operatingSystems;
    private final String[] servers;
    private final String[] jvmArgs;
    private final String[] jvms;
    private final String[] jvmVersions;
    private final String[] neo4jBranchOwners;
    private final String[] toolBranchOwners;
    private final boolean withPrintout;
    private final Repository[] tools;
    private final Repository[] projects;

    private SyntheticStoreGenerator(
            int days,
            int resultsPerDay,
            Group[] benchmarkGroups,
            String[] neo4jVersions,
            Edition[] neo4jEditions,
            int settingsInConfig,
            String[] operatingSystems,
            String[] servers,
            String[] jvmArgs,
            String[] jvms,
            String[] jvmVersions,
            String[] neo4jBranchOwners,
            String[] toolBranchOwners,
            Repository[] tools,
            Repository[] projects,
            boolean withPrintout )
    {
        this.days = days;
        this.resultsPerDay = resultsPerDay;
        this.benchmarkGroups = benchmarkGroups;
        this.neo4jVersions = neo4jVersions;
        this.neo4jEditions = neo4jEditions;
        this.settingsInConfig = settingsInConfig;
        this.operatingSystems = operatingSystems;
        this.servers = servers;
        this.jvmArgs = jvmArgs;
        this.jvms = jvms;
        this.jvmVersions = jvmVersions;
        this.neo4jBranchOwners = neo4jBranchOwners;
        this.toolBranchOwners = toolBranchOwners;
        this.tools = tools;
        this.projects = projects;
        this.withPrintout = withPrintout;
    }

    int resultCount()
    {
        return days * resultsPerDay;
    }

    public int days()
    {
        return days;
    }

    String[] neo4jBranchOwners()
    {
        return neo4jBranchOwners;
    }

    GenerationResult generate( StoreClient client )
    {
        GenerationResult generationResult = new GenerationResult();

        final Map<String,String> configMap = new HashMap<>();
        for ( int i = 0; i < settingsInConfig; i++ )
        {
            configMap.put( Integer.toString( i ), UUID.randomUUID().toString() );
        }
        final Neo4jConfig config = new Neo4jConfig( configMap );

        int minutesBetweenRuns = (int) TimeUnit.DAYS.toMinutes( 1 ) / resultsPerDay;
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis( System.currentTimeMillis() );
        calendar.set( Calendar.MILLISECOND, 0 );
        calendar.set( Calendar.SECOND, 0 );
        calendar.set( Calendar.MINUTE, 0 );
        calendar.set( Calendar.HOUR_OF_DAY, 0 );

        int runningCount = 0;
        long startClock = System.currentTimeMillis();
        long runningClock = startClock;
        Map<String,String> benchmarkConfigMap =
                IntStream.range( 1, 100 ).boxed().collect( toMap( i -> "key:" + i, i -> "value:" + i ) );
        BenchmarkConfig benchmarkConfig = new BenchmarkConfig( benchmarkConfigMap );
        for ( int day = 0; day < days; day++ )
        {
            for ( int dayResult = 0; dayResult < resultsPerDay; dayResult++ )
            {
                for ( Repository toolRepository : tools )
                {
                    BenchmarkTool tool = generateBenchmarkTool( toolRepository );
                    BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
                    for ( Group group : benchmarkGroups )
                    {
                        BenchmarkGroup benchmarkGroup = group.group();
                        for ( Benchmark benchmark : group.benchmarks() )
                        {
                            benchmarkGroupBenchmarkMetrics.add(
                                    benchmarkGroup,
                                    benchmark,
                                    new Metrics( UNIT.get(),
                                                 MIN_NS.get(),
                                                 MAX_NS.get(),
                                                 MEAN_NS.get(),
                                                 ERROR_NS.get(),
                                                 ERROR_CONFIDENCE.get(),
                                                 SAMPLE_SIZE,
                                                 PERC_25_NS.get(),
                                                 PERC_50_NS.get(),
                                                 PERC_75_NS.get(),
                                                 PERC_90_NS.get(),
                                                 PERC_95_NS.get(),
                                                 PERC_99_NS.get(),
                                                 PERC_99_9_NS.get() ),
                                    config );
                            generationResult.addBenchmark( tool, benchmarkGroup, benchmark );
                            generationResult.incMetrics();
                        }
                    }

                    calendar.add( Calendar.MINUTE, minutesBetweenRuns );
                    Project project = generateProject();
                    generationResult.addProject( project );
                    String triggeredBy = randomOwnerFor( project.repository() );
                    TestRun testRun = new TestRun( DURATION_MS.get(), calendar.getTimeInMillis(), BUILD.get(), BUILD.get(), triggeredBy );
                    generationResult.incTestRuns();

                    Environment environment = new Environment(
                            randomFrom( operatingSystems ),
                            randomFrom( servers ) );
                    generationResult.addEnvironments( environment );
                    Java java = new Java(
                            randomFrom( jvms ),
                            randomFrom( jvmVersions ),
                            randomFrom( jvmArgs ) );
                    generationResult.addJavas( java );
                    List<BenchmarkPlan> plans = new ArrayList<>();
                    List<TestRunError> errors = ERRORS.get();

                    TestRunReport testRunReport = new TestRunReport(
                            testRun,
                            benchmarkConfig,
                            Sets.newHashSet( project ),
                            config,
                            environment,
                            benchmarkGroupBenchmarkMetrics,
                            tool,
                            java,
                            plans,
                            errors );
                    SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport, Planner.RULE );

                    QueryRetrier queryRetrier = new QueryRetrier( withPrintout );
                    queryRetrier.execute( client, submitTestRun, 1 );

                    if ( RNG.nextDouble() > TEST_RUN_ANNOTATION_PROBABILITY )
                    {
                        AttachTestRunAnnotation attachTestRunAnnotation = new AttachTestRunAnnotation(
                                testRunReport.testRun().id(),
                                new Annotation( "comment", System.currentTimeMillis(), "author" ) );
                        generationResult.incTestRunAnnotations();
                        queryRetrier.execute( client, attachTestRunAnnotation, 1 );
                    }

                    for ( BenchmarkGroupBenchmark bgb : testRunReport.benchmarkGroupBenchmarks() )
                    {
                        if ( RNG.nextDouble() > METRICS_ANNOTATION_PROBABILITY )
                        {
                            queryRetrier.execute( client,
                                                  new AttachMetricsAnnotation( testRunReport.testRun().id(),
                                                                               bgb.benchmark().name(),
                                                                               bgb.benchmarkGroup().name(),
                                                                               new Annotation( "comment", System.currentTimeMillis(), "author" ) ),
                                                  1 );
                            generationResult.incMetricsAnnotations();
                        }
                    }
                }

                int i = generationResult.testRuns();
                if ( withPrintout && (i + 1) % 100 == 0 )
                {
                    int count = i + 1;
                    long now = System.currentTimeMillis();
                    double opsPerMs = (count - runningCount) / (double) (now - runningClock);
                    System.out.println( format( "Submitted %s / %s results : %s result/s",
                                                count, resultCount(), THROUGHPUT_FORMAT.format( opsPerMs * 1000 ) ) );
                    runningClock = now;
                    runningCount = count;
                }
            }
        }
        if ( withPrintout )
        {
            long durationMs = System.currentTimeMillis() - startClock;
            double opsPerMs = resultCount() / (double) durationMs;
            System.out.println( format( "------\nSubmitted: %s results\nDuration: %s\nThroughput: %s result/s\n------",
                                        resultCount(),
                                        BenchmarkUtil.durationToString( Duration.of( durationMs, ChronoUnit.MILLIS ) ),
                                        THROUGHPUT_FORMAT.format( opsPerMs * 1000 ) ) );
        }
        return generationResult;
    }

    private BenchmarkTool generateBenchmarkTool( Repository tool )
    {
        String owner = randomOwnerFor( tool );
        String commit = UUID.randomUUID().toString();
        String neo4jVersion = randomFrom( neo4jVersions );
        String branch = tool.isDefaultOwner( owner )
                        ? neo4jVersion.substring( 0, neo4jVersion.length() - 2 )
                        : neo4jVersion.substring( 0, neo4jVersion.length() - 2 ) + "-prototype";
        return new BenchmarkTool( tool, commit, owner, branch );
    }

    private Project generateProject()
    {
        Repository repository = randomFrom( projects );
        String owner = randomOwnerFor( repository );
        String commit = UUID.randomUUID().toString();
        String neo4jVersion = randomFrom( neo4jVersions );
        Edition neo4jEdition = randomFrom( neo4jEditions );
        String branch = BranchAndVersion.isPersonalBranch( repository, owner )
                        ? neo4jVersion + "-" + owner
                        : neo4jVersion;
        return new Project( repository, commit, neo4jVersion, neo4jEdition, branch, owner );
    }

    private String randomOwnerFor( Repository repository )
    {
        switch ( repository )
        {
        case NEO4J:
            return randomFrom( neo4jBranchOwners );
        case MICRO_BENCH:
        case MACRO_BENCH:
        case LDBC_BENCH:
        case IMPORT_BENCH:
            return randomFrom( toolBranchOwners );
        default:
            throw new IllegalArgumentException( "Unrecognized repository: " + repository );
        }
    }

    private static <T> T randomFrom( T[] array )
    {
        if ( array.length == 0 )
        {
            throw new IllegalArgumentException( "Empty array" );
        }
        else
        {
            try
            {
                int randomIndex = RNG.nextInt( array.length );
                return array[randomIndex];
            }
            catch ( NullPointerException e )
            {
                e.printStackTrace();
                throw e;
            }
        }
    }

    static class Group
    {
        static Group from( String groupName, int benchmarkCount )
        {
            String[] benchNames = IntStream.range( 0, benchmarkCount )
                                           .mapToObj( Integer::toString )
                                           .toArray( String[]::new );
            return Group.from( groupName, benchNames );
        }

        static Group from( String groupName, String... benchNames )
        {
            BenchmarkGroup group = new BenchmarkGroup( groupName );
            Benchmark[] benchmarks = Arrays.stream( benchNames )
                                           .map( b -> Benchmark
                                                   .benchmarkFor( "description for: " + b, b, randomFrom( Mode.values() ), BENCHMARK_PARAMETERS ) )
                                           .toArray( Benchmark[]::new );
            return from( group, benchmarks );
        }

        static Group from( BenchmarkGroup group, Benchmark... benchmarks )
        {
            return new Group( group, benchmarks );
        }

        private final BenchmarkGroup group;
        private final Benchmark[] benchmarks;

        private Group( BenchmarkGroup group, Benchmark... benchmarks )
        {
            this.group = group;
            this.benchmarks = benchmarks;
        }

        public BenchmarkGroup group()
        {
            return group;
        }

        public Benchmark[] benchmarks()
        {
            return benchmarks;
        }
    }

    static class GenerationResult
    {
        private Set<List<String>> benchmarkGroups = new HashSet<>();
        private Set<List<String>> benchmarks = new HashSet<>();
        private int testRuns;
        private Set<Environment> environments = new HashSet<>();
        private Set<Java> javas = new HashSet<>();
        private Set<Project> projects = new HashSet<>();
        private Set<String> tools = new HashSet<>();
        private Set<BenchmarkTool> toolVersions = new HashSet<>();
        private int metrics;
        private int testRunAnnotations;
        private int metricsAnnotations;

        private void addBenchmark( BenchmarkTool tool, BenchmarkGroup benchmarkGroup, Benchmark benchmark )
        {
            benchmarkGroups.add( Lists.newArrayList( tool.toolName(), benchmarkGroup.name() ) );
            benchmarks.add( Lists.newArrayList( tool.toolName(), benchmarkGroup.name(), benchmark.name() ) );
            tools.add( tool.toolName() );
            toolVersions.add( tool );
        }

        private void incTestRuns()
        {
            testRuns++;
        }

        private void addEnvironments( Environment environment )
        {
            environments.add( environment );
        }

        private void addJavas( Java java )
        {
            javas.add( java );
        }

        private void addProject( Project project )
        {
            projects.add( project );
        }

        private void incMetrics()
        {
            metrics++;
        }

        private void incTestRunAnnotations()
        {
            testRunAnnotations++;
        }

        private void incMetricsAnnotations()
        {
            metricsAnnotations++;
        }

        int benchmarkGroups()
        {
            return benchmarkGroups.size();
        }

        int benchmarks()
        {
            return benchmarks.size();
        }

        int benchmarksInTool( String tool )
        {
            return (int) benchmarks.stream()
                                   .filter( b -> b.get( 0 ).equalsIgnoreCase( tool ) )
                                   .count();
        }

        int benchmarksInToolAndGroups( String tool, String... groups )
        {
            return (int) benchmarks.stream()
                                   .filter( b -> b.get( 0 ).equalsIgnoreCase( tool ) && Arrays.stream( groups ).anyMatch( b.get( 1 )::equalsIgnoreCase ) )
                                   .count();
        }

        int testRuns()
        {
            return testRuns;
        }

        int environments()
        {
            return environments.size();
        }

        int javas()
        {
            return javas.size();
        }

        int projects()
        {
            return projects.size();
        }

        int baseNeo4jConfigs()
        {
            return testRuns();
        }

        int neo4jConfigs()
        {
            return testRuns() + metrics();
        }

        int toolVersions()
        {
            return toolVersions.size();
        }

        int tools()
        {
            return tools.size();
        }

        int metrics()
        {
            return metrics;
        }

        int testRunAnnotations()
        {
            return testRunAnnotations;
        }

        int metricsAnnotations()
        {
            return metricsAnnotations;
        }
    }
}
