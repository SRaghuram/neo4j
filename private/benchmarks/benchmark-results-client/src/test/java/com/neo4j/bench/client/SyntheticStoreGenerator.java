/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.queries.AttachMetricsAnnotation;
import com.neo4j.bench.client.queries.AttachTestRunAnnotation;
import com.neo4j.bench.client.queries.SubmitTestRun;
import com.neo4j.bench.client.queries.SubmitTestRunResult;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.neo4j.bench.common.model.Benchmark.Mode.LATENCY;
import static com.neo4j.bench.common.model.Benchmark.Mode.SINGLE_SHOT;
import static com.neo4j.bench.common.model.Benchmark.Mode.THROUGHPUT;
import static com.neo4j.bench.common.model.Repository.ALGOS;
import static com.neo4j.bench.common.model.Repository.ALGOS_JMH;
import static com.neo4j.bench.common.model.Repository.CAPS;
import static com.neo4j.bench.common.model.Repository.CAPS_BENCH;
import static com.neo4j.bench.common.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.common.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.common.model.Repository.MACRO_BENCH;
import static com.neo4j.bench.common.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.common.model.Repository.MORPHEUS_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
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
    private static final String[] DEFAULT_BENCHMARK_GROUP_COUNT = {"1", "2", "3", "4"};
    private static final int DEFAULT_BENCHMARK_PER_GROUP_COUNT = 10;
    private static final String[] DEFAULT_NEO4J_VERSIONS = {"3.0.2", "3.0.1", "3.0.0"};
    private static final Edition[] DEFAULT_NEO4J_EDITIONS = Edition.values();
    private static final int DEFAULT_SETTINGS_IN_CONFIG = 50;
    private static final Repository[] TOOLS = {MICRO_BENCH, MACRO_BENCH, LDBC_BENCH, IMPORT_BENCH, CAPS_BENCH, MORPHEUS_BENCH, ALGOS_JMH};
    private static final Repository[] PROJECTS = {CAPS, NEO4J, ALGOS};
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

    private static final Map<String,String> BENCHMARK_PARAMETERS =
            IntStream.range( 0, 10 ).boxed().map( Object::toString ).collect( toMap( s -> "k_" + s, s -> "v_" + s ) );

    private static final long SAMPLE_SIZE = 10_000;

    public static class SyntheticStoreGeneratorBuilder
    {
        private int days = DEFAULT_DAYS;
        private int resultsPerDay = DEFAULT_RESULTS_PER_DAY;
        private String[] benchmarkGroupNames = DEFAULT_BENCHMARK_GROUP_COUNT;
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
        private Repository[] tools = TOOLS;
        private Repository[] projects = PROJECTS;
        private boolean withPrintout;
        private boolean withAssertions = true;

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

        SyntheticStoreGeneratorBuilder withBenchmarkGroups( String... benchmarkGroupNames )
        {
            this.benchmarkGroupNames = benchmarkGroupNames;
            return this;
        }

        SyntheticStoreGeneratorBuilder withBenchmarkPerGroupCount( int benchmarkPerGroupCount )
        {
            this.benchmarkPerGroupCount = benchmarkPerGroupCount;
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

        SyntheticStoreGeneratorBuilder withCapsBranchOwners( String... capsBranchOwners )
        {
            this.capsBranchOwners = capsBranchOwners;
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

        SyntheticStoreGeneratorBuilder withAssertions( boolean withAssertions )
        {
            this.withAssertions = withAssertions;
            return this;
        }

        public SyntheticStoreGenerator build()
        {
            return new SyntheticStoreGenerator(
                    days,
                    resultsPerDay,
                    benchmarkGroupNames,
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
                    tools,
                    projects,
                    withPrintout,
                    withAssertions
            );
        }
    }

    private final int days;

    private final int resultsPerDay;
    private final BenchmarkGroup[] benchmarkGroupMapping;
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
    private final Repository[] tools;
    private final Repository[] projects;

    private SyntheticStoreGenerator(
            int days,
            int resultsPerDay,
            String[] benchmarkGroupNames,
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
            Repository[] tools,
            Repository[] projects,
            boolean withPrintout,
            boolean withAssertions )
    {
        this.days = days;
        this.resultsPerDay = resultsPerDay;
        this.benchmarkGroupMapping = Arrays.stream( benchmarkGroupNames ).map( BenchmarkGroup::new ).toArray( BenchmarkGroup[]::new );
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
        this.tools = tools;
        this.projects = projects;
        this.withPrintout = withPrintout;
        this.withAssertions = withAssertions;
    }

    int resultCount()
    {
        return days * resultsPerDay;
    }

    int maxNumberOfBenchmarkTools()
    {
        return tools.length * benchmarkGroupCount();
    }

    public int days()
    {
        return days;
    }

    int resultsPerDay()
    {
        return resultsPerDay;
    }

    int benchmarkGroupCount()
    {
        return benchmarkGroupMapping.length;
    }

    int benchmarkPerGroupCount()
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

    String[] operatingSystems()
    {
        return operatingSystems;
    }

    String[] servers()
    {
        return servers;
    }

    String[] jvmArgs()
    {
        return jvmArgs;
    }

    String[] jvms()
    {
        return jvms;
    }

    String[] jvmVersions()
    {
        return jvmVersions;
    }

    String[] neo4jBranchOwners()
    {
        return neo4jBranchOwners;
    }

    String[] capsBranchOwners()
    {
        return capsBranchOwners;
    }

    void generate( StoreClient client )
    {
        final Map<String,String> configMap = new HashMap<>();
        for ( int i = 0; i < settingsInConfig; i++ )
        {
            configMap.put( Integer.toString( i ), UUID.randomUUID().toString() );
        }
        final Neo4jConfig config = new Neo4jConfig( configMap );
        final Benchmark[][] benchmarkMapping = new Benchmark[benchmarkGroupCount()][benchmarkPerGroupCount];
        for ( int groupId = 0; groupId < benchmarkGroupCount(); groupId++ )
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
                int benchmarkGroupId = RNG.nextInt( benchmarkGroupCount() );
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
                String triggeredBy = randomOwnerFor( randomFrom( projects ) );
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
                SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport, Planner.RULE );

                QueryRetrier queryRetrier = new QueryRetrier( withPrintout );
                SubmitTestRunResult result = queryRetrier.execute( client, submitTestRun, 1 );

                if ( RNG.nextDouble() > TEST_RUN_ANNOTATION_PROBABILITY )
                {
                    AttachTestRunAnnotation attachTestRunAnnotation = new AttachTestRunAnnotation(
                            testRunReport.testRun().id(),
                            new Annotation( "comment", System.currentTimeMillis(), "author" ) );
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
            long durationMs = System.currentTimeMillis() - startClock;
            double opsPerMs = resultCount() / (double) durationMs;
            System.out.println( format( "------\nSubmitted: %s results\nDuration: %s\nThroughput: %s result/s\n------",
                                        resultCount(),
                                        BenchmarkUtil.durationToString( Duration.of( durationMs, ChronoUnit.MILLIS ) ),
                                        THROUGHPUT_FORMAT.format( opsPerMs * 1000 ) ) );
        }
    }

    private BenchmarkTool generateBenchmarkTool()
    {
        Repository tool = randomFrom( tools );
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
        Repository repository = randomFrom( projects );
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
