/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.util;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.ClientUtil;
import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.model.Annotation;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.Benchmark.Mode;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmark;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkPlan;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.BranchAndVersion;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.Project;
import com.neo4j.bench.client.model.Repository;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunError;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.options.Planner;
import com.neo4j.bench.client.queries.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.AttachTestRunAnnotation;
import com.neo4j.bench.client.queries.SubmitTestRun;
import com.neo4j.bench.client.queries.SubmitTestRunResult;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.neo4j.bench.client.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.client.model.Benchmark.Mode.SINGLE_SHOT;
import static com.neo4j.bench.client.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.client.model.Repository.CAPS;
import static com.neo4j.bench.client.model.Repository.CAPS_BENCH;
import static com.neo4j.bench.client.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.client.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.client.model.Repository.NEO4J;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SyntheticStoreGenerator
{
    private static final double TEST_RUN_ANNOTATION_PROBABILITY = 0.5;
    private static final double METRICS_ANNOTATION_PROBABILITY = 0.5;
    private static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat( "#,###,##0.00" );
    private static final int DEFAULT_DAYS = 7;
    private static final int DEFAULT_RESULTS_PER_DAY = 10;
    private static final int DEFAULT_BENCHMARK_GROUP_COUNT = 4;
    private static final int DEFAULT_BENCHMARK_PER_GROUP_COUNT = 10;
    private static final String[] DEFAULT_NEO4J_VERSIONS = {"3.0.2", "3.0.1", "3.0.0"};
    private static final Edition[] DEFAULT_NEO4J_EDITIONS = Edition.values();
    private static final int DEFAULT_SETTINGS_IN_CONFIG = 50;
    private static final Repository[] TOOLS = {MICRO_BENCH, LDBC_BENCH, CAPS_BENCH};
    private static final Repository[] PROJECTS = {NEO4J, CAPS};
    private static final String[] DEFAULT_OPERATING_SYSTEMS = {"Windows", "OSX", "Ubuntu"};
    private static final String[] DEFAULT_SERVERS = {"Skalleper", "local", "AWS", "Mattis", "Borka"};
    private static final String[] DEFAULT_JVM_ARGS = {"-XX:+UseG1GC -Xmx4g", "-server", "-Xmx12g"};
    private static final String[] DEFAULT_JVMS = {"Oracle", "OpenJDK"};
    private static final String[] DEFAULT_JVM_VERSIONS = {"1.80_66", "1.80_12", "1.7.0_42"};
    private static final String[] DEFAULT_NEO4J_BRANCH_OWNERS = {NEO4J.defaultOwner()};
    private static final String[] DEFAULT_CAPS_BRANCH_OWNERS = {CAPS.defaultOwner()};
    private static final String[] DEFAULT_TOOL_BRANCH_OWNERS = {
            MICRO_BENCH.defaultOwner(),
            LDBC_BENCH.defaultOwner(),
            CAPS_BENCH.defaultOwner()};
    private static final Mode[] MODES = {THROUGHPUT, LATENCY, SINGLE_SHOT};
    private static final TimeUnit[] UNITS = new TimeUnit[]{SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS};
    private static final String BENCHMARK_DESCRIPTION =
            IntStream.range( 0, 50 ).mapToObj( i -> "description" ).collect( joining() );

    private static final RichRandom RNG = new RichRandom( 42 );
    private static final Supplier<TimeUnit> UNIT = () -> UNITS[RNG.nextInt( 0, UNITS.length - 1 )];
    private static final Supplier<Mode> MODE = () -> MODES[RNG.nextInt( 0, MODES.length - 1 )];
    private static final Supplier<Double> MIN_NS = () -> RNG.nextDouble( 1, 100 );
    private static final Supplier<Double> MEAN_NS = () -> RNG.nextGaussian() * 300 + 500;
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
    private static final Supplier<Repository> TOOL = () -> TOOLS[RNG.nextInt( 0, TOOLS.length - 1 )];
    private static final Supplier<Repository> PROJECT = () -> PROJECTS[RNG.nextInt( 0, PROJECTS.length - 1 )];
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

    private static final Map<String,String> BENCHMARK_PARAMETERS =
            IntStream.range( 0, 10 ).boxed().map( Object::toString ).collect( toMap( s -> "k_" + s, s -> "v_" + s ) );

    private static final long SAMPLE_SIZE = 10_000;

    public static class SyntheticStoreGeneratorBuilder
    {
        private int days = DEFAULT_DAYS;
        private int resultsPerDay = DEFAULT_RESULTS_PER_DAY;
        private int benchmarkGroupCount = DEFAULT_BENCHMARK_GROUP_COUNT;
        private int benchmarkPerGroupCount = DEFAULT_BENCHMARK_PER_GROUP_COUNT;
        private String[] neo4jVersions = DEFAULT_NEO4J_VERSIONS;
        private Edition[] neo4jEditions = DEFAULT_NEO4J_EDITIONS;
        private int settingsInConfig = DEFAULT_SETTINGS_IN_CONFIG;
        private String[] operatingSystems = DEFAULT_OPERATING_SYSTEMS;
        private String[] servers = DEFAULT_SERVERS;
        private String[] jvmArgs = DEFAULT_JVM_ARGS;
        private String[] jvms = DEFAULT_JVMS;
        private String[] jvmVersions = DEFAULT_JVM_VERSIONS;
        private String[] neo4jBranchOwners = DEFAULT_NEO4J_BRANCH_OWNERS;
        private String[] capsBranchOwners = DEFAULT_CAPS_BRANCH_OWNERS;
        private String[] toolBranchOwners = DEFAULT_TOOL_BRANCH_OWNERS;
        private boolean withPrintout;
        private boolean withAssertions = true;

        public SyntheticStoreGeneratorBuilder withDays( int days )
        {
            this.days = days;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withResultsPerDay( int resultsPerDay )
        {
            this.resultsPerDay = resultsPerDay;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withBenchmarkGroupCount( int benchmarkGroupCount )
        {
            this.benchmarkGroupCount = benchmarkGroupCount;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withBenchmarkPerGroupCount( int benchmarkPerGroupCount )
        {
            this.benchmarkPerGroupCount = benchmarkPerGroupCount;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withNeo4jVersions( String... neo4jVersions )
        {
            this.neo4jVersions = neo4jVersions;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withNeo4jEditions( Edition... neo4jEditions )
        {
            this.neo4jEditions = neo4jEditions;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withSettingsInConfig( int settingsInConfig )
        {
            this.settingsInConfig = settingsInConfig;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withOperatingSystems( String... operatingSystems )
        {
            this.operatingSystems = operatingSystems;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withServers( String... servers )
        {
            this.servers = servers;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withJvmArgs( String... jvmArgs )
        {
            this.jvmArgs = jvmArgs;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withJvms( String... jvms )
        {
            this.jvms = jvms;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withJvmVersions( String... jvmVersions )
        {
            this.jvmVersions = jvmVersions;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withNeo4jBranchOwners( String... neo4jBranchOwners )
        {
            this.neo4jBranchOwners = neo4jBranchOwners;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withCapsBranchOwners( String... capsBranchOwners )
        {
            this.capsBranchOwners = capsBranchOwners;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withToolBranchOwners( String... toolBranchOwners )
        {
            this.toolBranchOwners = toolBranchOwners;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withPrintout( boolean withPrintout )
        {
            this.withPrintout = withPrintout;
            return this;
        }

        public SyntheticStoreGeneratorBuilder withAssertions( boolean withAssertions )
        {
            this.withAssertions = withAssertions;
            return this;
        }

        public SyntheticStoreGenerator build()
        {
            return new SyntheticStoreGenerator(
                    days,
                    resultsPerDay,
                    benchmarkGroupCount,
                    benchmarkPerGroupCount,
                    neo4jVersions,
                    neo4jEditions,
                    settingsInConfig,
                    operatingSystems,
                    servers,
                    jvmArgs,
                    jvms,
                    jvmVersions,
                    neo4jBranchOwners,
                    capsBranchOwners,
                    toolBranchOwners,
                    withPrintout,
                    withAssertions
            );
        }
    }

    private final int days;

    private final int resultsPerDay;
    private final int benchmarkGroupCount;
    private final int benchmarkPerGroupCount;
    private final String[] neo4jVersions;
    private final Edition[] neo4jEditions;
    private final int settingsInConfig;
    private final String[] operatingSystems;
    private final String[] servers;
    private final String[] jvmArgs;
    private final String[] jvms;
    private final String[] jvmVersions;
    private final String[] neo4jBranchOwners;
    private final String[] capsBranchOwners;
    private final String[] toolBranchOwners;
    private final boolean withPrintout;
    private final boolean withAssertions;

    private SyntheticStoreGenerator(
            int days,
            int resultsPerDay,
            int benchmarkGroupCount,
            int benchmarkPerGroupCount,
            String[] neo4jVersions,
            Edition[] neo4jEditions,
            int settingsInConfig,
            String[] operatingSystems,
            String[] servers,
            String[] jvmArgs,
            String[] jvms,
            String[] jvmVersions,
            String[] neo4jBranchOwners,
            String[] capsBranchOwners,
            String[] toolBranchOwners,
            boolean withPrintout,
            boolean withAssertions )
    {
        this.days = days;
        this.resultsPerDay = resultsPerDay;
        this.benchmarkGroupCount = benchmarkGroupCount;
        this.benchmarkPerGroupCount = benchmarkPerGroupCount;
        this.neo4jVersions = neo4jVersions;
        this.neo4jEditions = neo4jEditions;
        this.settingsInConfig = settingsInConfig;
        this.operatingSystems = operatingSystems;
        this.servers = servers;
        this.jvmArgs = jvmArgs;
        this.jvms = jvms;
        this.jvmVersions = jvmVersions;
        this.neo4jBranchOwners = neo4jBranchOwners;
        this.capsBranchOwners = capsBranchOwners;
        this.toolBranchOwners = toolBranchOwners;
        this.withPrintout = withPrintout;
        this.withAssertions = withAssertions;
    }

    public int resultCount()
    {
        return days * resultsPerDay;
    }

    public int maxNumberOfBenchmarkTools()
    {
        return TOOLS.length * benchmarkGroupCount;
    }

    public int days()
    {
        return days;
    }

    public int resultsPerDay()
    {
        return resultsPerDay;
    }

    public int benchmarkGroupCount()
    {
        return benchmarkGroupCount;
    }

    public int benchmarkPerGroupCount()
    {
        return benchmarkPerGroupCount;
    }

    public String[] neo4jVersions()
    {
        return neo4jVersions;
    }

    public Edition[] neo4jEditions()
    {
        return neo4jEditions;
    }

    public int settingsInConfig()
    {
        return settingsInConfig;
    }

    public String[] operatingSystems()
    {
        return operatingSystems;
    }

    public String[] servers()
    {
        return servers;
    }

    public String[] jvmArgs()
    {
        return jvmArgs;
    }

    public String[] jvms()
    {
        return jvms;
    }

    public String[] jvmVersions()
    {
        return jvmVersions;
    }

    public String[] neo4jBranchOwners()
    {
        return neo4jBranchOwners;
    }

    public String[] capsBranchOwners()
    {
        return capsBranchOwners;
    }

    public void generate( StoreClient client )
    {
        final Map<String,String> configMap = new HashMap<>();
        for ( int i = 0; i < settingsInConfig; i++ )
        {
            configMap.put( Integer.toString( i ), UUID.randomUUID().toString() );
        }
        final Neo4jConfig config = new Neo4jConfig( configMap );
        final BenchmarkGroup[] benchmarkGroupMapping = new BenchmarkGroup[benchmarkGroupCount];
        final Benchmark[][] benchmarkMapping = new Benchmark[benchmarkGroupCount][benchmarkPerGroupCount];
        for ( int groupId = 0; groupId < benchmarkGroupCount; groupId++ )
        {
            benchmarkGroupMapping[groupId] = new BenchmarkGroup( Integer.toString( groupId ) );
            for ( int benchmarkId = 0; benchmarkId < benchmarkPerGroupCount; benchmarkId++ )
            {
                String simpleBenchmarkName = Integer.toString( benchmarkId );
                String benchmarkDescription = simpleBenchmarkName + BENCHMARK_DESCRIPTION;
                benchmarkMapping[groupId][benchmarkId] = Benchmark.benchmarkFor(
                        benchmarkDescription,
                        simpleBenchmarkName,
                        MODE.get(),
                        BENCHMARK_PARAMETERS );
            }
        }

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
                BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
                int benchmarkGroupId = RNG.nextInt( benchmarkGroupCount );
                BenchmarkGroup benchmarkGroup = benchmarkGroupMapping[benchmarkGroupId];
                for ( Benchmark benchmark : benchmarkMapping[benchmarkGroupId] )
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
                }
                calendar.add( Calendar.MINUTE, minutesBetweenRuns );
                String triggeredBy = randomOwnerFor( PROJECT.get() );
                TestRun testRun =
                        new TestRun( DURATION_MS.get(), calendar.getTimeInMillis(), BUILD.get(), BUILD.get(), triggeredBy );

                BenchmarkTool tool = generateBenchmarkTool();
                Set<Project> project = generateProjects();
                Environment environment = new Environment(
                        randomFrom( operatingSystems ),
                        randomFrom( servers ) );
                Java java = new Java(
                        randomFrom( jvms ),
                        randomFrom( jvmVersions ),
                        randomFrom( jvmArgs ) );
                List<BenchmarkPlan> plans = new ArrayList<>();
                List<TestRunError> errors = ERRORS.get();

                TestRunReport testRunReport = new TestRunReport(
                        testRun,
                        benchmarkConfig,
                        project,
                        config,
                        environment,
                        benchmarkGroupBenchmarkMetrics,
                        tool,
                        java,
                        plans,
                        errors );
                SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport, Planner.COST );

                SubmitTestRunResult result = new QueryRetrier().execute( client, submitTestRun, 1 );

                if ( RNG.nextDouble() > TEST_RUN_ANNOTATION_PROBABILITY )
                {
                    AttachTestRunAnnotation attachTestRunAnnotation = new AttachTestRunAnnotation(
                            testRunReport.testRun().id(),
                            new Annotation( "comment", System.currentTimeMillis(), "author" ) );
                    new QueryRetrier().execute( client, attachTestRunAnnotation, 1 );
                }

                for ( BenchmarkGroupBenchmark bgb : testRunReport.benchmarkGroupBenchmarks() )
                {
                    if ( RNG.nextDouble() > METRICS_ANNOTATION_PROBABILITY )
                    {
                        new QueryRetrier().execute( client,
                                                    new AttachMetricsAnnotation( testRunReport.testRun().id(),
                                                                                 bgb.benchmark().name(),
                                                                                 bgb.benchmarkGroup().name(),
                                                                                 new Annotation( "comment", System.currentTimeMillis(), "author" ) ),
                                                    1 );
                    }
                }

                if ( withAssertions )
                {
                    if ( result.benchmarkMetricsList().size() != benchmarkPerGroupCount )
                    {
                        throw new AssertionError( format( "%s <> %s\nCreate/return one metrics per benchmark in group",
                                                          result.benchmarkMetricsList().size(),
                                                          benchmarkPerGroupCount ) );
                    }
                    if ( !result.testRun().id().equals( testRun.id() ) )
                    {
                        throw new AssertionError( "return result for same test run" );
                    }
                    Set<String> createdBenchmarks = result.benchmarkMetricsList().stream()
                                                          .map( bm -> bm.benchmark().name() ).collect( Collectors.toSet() );
                    if ( !Arrays.stream( benchmarkMapping[benchmarkGroupId] )
                                .allMatch( b -> createdBenchmarks.contains( b.name() ) ) )
                    {
                        throw new AssertionError( "create a result for every benchmark" );
                    }
                }

                int i = (day * resultsPerDay) + dayResult;
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
            long duration = System.currentTimeMillis() - startClock;
            double opsPerMs = resultCount() / (double) duration;
            System.out.println( format( "------\nSubmitted: %s results\nDuration: %s\nThroughput: %s result/s\n------",
                                        resultCount(),
                                        ClientUtil.durationToString( duration ),
                                        THROUGHPUT_FORMAT.format( opsPerMs * 1000 ) ) );
        }
    }

    private BenchmarkTool generateBenchmarkTool()
    {
        Repository tool = TOOL.get();
        String owner = randomOwnerFor( tool );
        String commit = UUID.randomUUID().toString();
        String neo4jVersion = randomFrom( neo4jVersions );
        String branch = tool.isDefaultOwner( owner )
                        ? neo4jVersion.substring( 0, neo4jVersion.length() - 2 )
                        : neo4jVersion.substring( 0, neo4jVersion.length() - 2 ) + "-prototype";
        return new BenchmarkTool( tool, commit, owner, branch );
    }

    private Set<Project> generateProjects()
    {
        Repository repository = PROJECT.get();
        String owner = randomOwnerFor( repository );
        String commit = UUID.randomUUID().toString();
        String neo4jVersion = randomFrom( neo4jVersions );
        Edition neo4jEdition = randomFrom( neo4jEditions );
        String branch = BranchAndVersion.isPersonalBranch( repository, owner )
                        ? neo4jVersion + "-" + owner
                        : neo4jVersion;
        return Sets.newHashSet( new Project( repository, commit, neo4jVersion, neo4jEdition, branch, owner ) );
    }

    private String randomOwnerFor( Repository repository )
    {
        switch ( repository )
        {
        case NEO4J:
            return randomFrom( neo4jBranchOwners );
        case CAPS:
            return randomFrom( capsBranchOwners );
        case CAPS_BENCH:
            return randomFrom( toolBranchOwners );
        case MICRO_BENCH:
            return randomFrom( toolBranchOwners );
        case LDBC_BENCH:
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
            int randomIndex = RNG.nextInt( array.length );
            return array[randomIndex];
        }
    }
}
