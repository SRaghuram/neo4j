/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.common.util.TestSupport;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.Plan;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class PlannerDescriptionIT
{
    private static final Logger LOG = LoggerFactory.getLogger( PlannerDescriptionIT.class );
    private static final int BOLT_PORT = PortAuthority.allocatePort();
    private static final int HTTP_PORT = PortAuthority.allocatePort();
    private static final String QUERY = "PROFILE MATCH (a:A), (b:B) WHERE a.x = b.x AND b.y='foo' AND (a)--(b) RETURN count(a)";
    private static final String EXPECTED_ASCII_PLAN = "+--------------------+------------------------+----------------+------+\n" +
                                                      "| Operator           | Details                | Estimated Rows | Rows |\n" +
                                                      "+--------------------+------------------------+----------------+------+\n" +
                                                      "| +ProduceResults    | `count(a)`             |              1 |    1 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +EagerAggregation  | count(a) AS `count(a)` |              1 |    1 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +Apply             |                        |              0 |    0 |\n" +
                                                      "| |\\                 +------------------------+----------------+------+\n" +
                                                      "| | +Limit           | 1                      |              0 |    0 |\n" +
                                                      "| | |                +------------------------+----------------+------+\n" +
                                                      "| | +Expand(Into)    | (a)-[anon_57]-(b)      |              0 |    0 |\n" +
                                                      "| | |                +------------------------+----------------+------+\n" +
                                                      "| | +EmptyRow        | a, b                   |              1 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +ValueHashJoin     | b.x = a.x              |              0 |    0 |\n" +
                                                      "| |\\                 +------------------------+----------------+------+\n" +
                                                      "| | +NodeByLabelScan | a:A                    |             10 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +Filter            | b.y = $autostring_0    |              0 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +NodeByLabelScan   | b:B                    |             10 |    0 |\n" +
                                                      "+--------------------+------------------------+----------------+------+\n";

    private static final String EXPECTED_ASCII_PLAN_SERVER = "+--------------------------+------------------------+----------------+------+\n" +
                                                             "| Operator                 | Details                | Estimated Rows | Rows |\n" +
                                                             "+--------------------------+------------------------+----------------+------+\n" +
                                                             "| +ProduceResults@neo4j    | `count(a)`             |              1 |    1 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +EagerAggregation@neo4j  | count(a) AS `count(a)` |              1 |    1 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +Apply@neo4j             |                        |              0 |    0 |\n" +
                                                             "| |\\                       +------------------------+----------------+------+\n" +
                                                             "| | +Limit@neo4j           | 1                      |              0 |    0 |\n" +
                                                             "| | |                      +------------------------+----------------+------+\n" +
                                                             "| | +Expand(Into)@neo4j    | (a)-[anon_57]-(b)      |              0 |    0 |\n" +
                                                             "| | |                      +------------------------+----------------+------+\n" +
                                                             "| | +Argument@neo4j        | a, b                   |              1 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +ValueHashJoin@neo4j     | b.x = a.x              |              0 |    0 |\n" +
                                                             "| |\\                       +------------------------+----------------+------+\n" +
                                                             "| | +NodeByLabelScan@neo4j | a:A                    |             10 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +Filter@neo4j            | b.y = $autostring_0    |              0 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +NodeByLabelScan@neo4j   | b:B                    |             10 |    0 |\n" +
                                                             "+--------------------------+------------------------+----------------+------+\n";

    private final String randId = UUID.randomUUID().toString();

    @Inject
    private TestDirectory temporaryFolder;
    private Path neo4jDir;

    private static Jvm getJvm()
    {
        return Jvm.defaultJvmOrFail();
    }

    @BeforeEach
    public void copyNeo4j() throws Exception
    {
        Path originalDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
        this.neo4jDir = temporaryFolder.directory( format( "neo4jDir-%s", randId ) );
        FileUtils.copyDirectory( originalDir.toFile(), neo4jDir.toFile() );
    }

    @AfterEach
    public void deleteTemporaryNeo4j() throws IOException
    {
        FileUtils.deleteDirectory( neo4jDir.toFile() );
    }

    @Test
    public void shouldExtractPlansViaEmbedded()
    {
        Path neo4jConfigFile = writeEmbeddedNeo4jConfig();
        try ( Resources resources = new Resources( temporaryFolder.directory( format( "resources-%s", randId ) ) ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
                Path storePath = temporaryFolder.directory( format( "store-%s-%s", workload.name(), randId ) );
                try ( Store store = StoreTestUtil.createTemporaryEmptyStoreFor( workload, storePath, neo4jConfigFile );
                      EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile ) )
                {
                    for ( Query query : workload.queries() )
                    {
                        try ( org.neo4j.graphdb.Transaction tx = database.inner().beginTx() )
                        {
                            Result result = tx.execute( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                            result.accept( row -> true );
                            ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
                            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
                            assertPlansEqual( rootPlanOperator, rootPlanDescription, new EmbeddedPlanDescriptionAccessors() );
                        }
                        catch ( Exception e )
                        {
                            throw new RuntimeException( format( "Plans comparison failed!\n" +
                                                                "Workload: %s\n" +
                                                                "Query:    %s", workload.name(), query.name() ), e );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldExtractPlansViaDriver() throws IOException, TimeoutException
    {
        Jvm jvm = getJvm();

        try ( Resources resources = new Resources( temporaryFolder.directory( format( "resources-%s", randId ) ) ) )
        {
            for ( Workload workload : Workload.all( resources, Deployment.embedded() ) )
            {
                LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
                Redirect outputRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-out-%s-%s.log", workload.name(), randId ) ).toFile() );
                Redirect errorRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-error-%s-%s.log", workload.name(), randId ) ).toFile() );
                Path logsDir = Files.createDirectories( temporaryFolder.directory( format( "logs-%s-%s", workload.name(), randId ) ) );
                Path neo4jConfigFile = writeServerNeo4jConfig( workload.name() );
                Path storePath = temporaryFolder.directory( format( "store-%s-%s", workload.name(), randId ) );
                try ( Store store = StoreTestUtil.createEmptyStoreFor( workload, storePath, neo4jConfigFile );
                      ServerDatabase database = ServerDatabase.startServer( jvm,
                                                                            neo4jDir,
                                                                            store,
                                                                            neo4jConfigFile,
                                                                            outputRedirect,
                                                                            errorRedirect,
                                                                            logsDir ) )
                {
                    for ( Query query : workload.queries() )
                    {
                        try ( Transaction tx = database.session().beginTransaction() )
                        {
                            org.neo4j.driver.Result result = tx.run( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                            Plan rootDriverPlan = result.consume().plan();
                            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootDriverPlan );
                            assertPlansEqual( rootPlanOperator, rootDriverPlan, new DriverPlanAccessors() );
                        }
                        catch ( Exception e )
                        {
                            throw new RuntimeException( format( "Plans comparison failed!\n" +
                                                                "Workload: %s\n" +
                                                                "Query:    %s", workload.name(), query.name() ), e );
                        }
                    }
                }
            }
        }
    }

    @Test
    public void shouldRenderAsciiPlanFromEmbedded()
    {
        Path neo4jConfigFile = writeEmbeddedNeo4jConfig();
        try ( Store store = TestSupport.createTemporaryEmptyStore( temporaryFolder.directory( format( "store-%s", randId ) ), neo4jConfigFile );
              EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile );
              org.neo4j.graphdb.Transaction tx = database.inner().beginTx() )
        {
            Result result = tx.execute( QUERY );
            result.accept( row -> true );
            ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
            String asciiPlan = PlannerDescription.toAsciiPlan( rootPlanOperator );
            assertThat( asciiPlan, equalTo( EXPECTED_ASCII_PLAN ) );
        }
    }

    @Test
    public void shouldRenderAsciiPlanFromDriver() throws Exception
    {
        Jvm jvm = getJvm();
        Path neo4jConfigFile = writeServerNeo4jConfig( "ascii" );

        Redirect outputRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-out-%s.log", randId ) ).toFile() );
        Redirect errorRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-error-%s.log", randId ) ).toFile() );
        Path logsDir = Files.createDirectories( temporaryFolder.directory( format( "logs-%s", randId ) ) );

        try ( Store store = TestSupport.createTemporaryEmptyStore( temporaryFolder.directory( format( "store-%s", randId ) ), neo4jConfigFile );
              ServerDatabase database = ServerDatabase.startServer( jvm,
                                                                    neo4jDir,
                                                                    store,
                                                                    neo4jConfigFile,
                                                                    outputRedirect,
                                                                    errorRedirect,
                                                                    logsDir );
              Transaction tx = database.session().beginTransaction() )
        {
            org.neo4j.driver.Result result = tx.run( QUERY );
            Plan rootDriverPlan = result.consume().plan();
            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootDriverPlan );
            String asciiPlan = PlannerDescription.toAsciiPlan( rootPlanOperator );
            assertThat( asciiPlan, equalTo( EXPECTED_ASCII_PLAN_SERVER ) );
        }
    }

    private Path writeEmbeddedNeo4jConfig()
    {
        Path neo4jConfigFile = neo4jDir.resolve( "conf" ).resolve( format( "neo4j-%s.conf", randId ) );
        Neo4jConfigBuilder.withDefaults()
                          .writeToFile( neo4jConfigFile );
        return neo4jConfigFile;
    }

    private Path writeServerNeo4jConfig( String name )
    {
        Path neo4jConfigFile = neo4jDir.resolve( "conf" ).resolve( format( "neo4j-%s-%s.conf", name, randId ) );
        Neo4jConfigBuilder.withDefaults()
                          .withSetting( BoltConnector.listen_address, ":" + BOLT_PORT )
                          .withSetting( HttpConnector.enabled, "true" )
                          .withSetting( HttpConnector.listen_address, ":" + HTTP_PORT )
                          .writeToFile( neo4jConfigFile );
        return neo4jConfigFile;
    }

    private static final String ERROR_IN_TEST_CODE = "Error is probably in test code that traverses logical plan";

    private static <PLAN> void assertPlansEqual( PlanOperator rootPlanOperator, PLAN planDescriptionRoot, PlanAccessors<PLAN> planAccessors )
    {
        Stack<PlanOperator> planOperators = new Stack<>();
        Stack<PLAN> planDescriptions = new Stack<>();

        Map<PlanOperator,PlanOperator> parentMap = new HashMap<>();
        Map<PlanOperator,Set<PlanOperator>> childrenMap = new HashMap<>();

        int expectedPlanCount = operatorPlanCountFor( planDescriptionRoot, rootPlanOperator, planAccessors );
        int actualVisitedPlans = 0;

        planOperators.push( rootPlanOperator );
        planDescriptions.push( planDescriptionRoot );

        while ( !planOperators.isEmpty() )
        {
            // plan representations should have the same size
            assertFalse( planDescriptions.isEmpty(), ERROR_IN_TEST_CODE );

            PlanOperator planOperator = planOperators.pop();
            PLAN planDescription = planDescriptions.pop();

            List<PlanOperator> planOperatorChildren = planOperator.children();
            List<PLAN> planDescriptionChildren = planAccessors.getChildren( planDescription );

            String planDescriptionName = planAccessors.getFullName( planDescription );
            String planOperatorName = fullNameFor( planOperator );

            // not yet a leaf operator, continue traversing down the plan
            if ( !childrenMap.containsKey( planOperator ) )
            {
                planOperators.push( planOperator );
                planDescriptions.push( planDescription );

                childrenMap.put( planOperator, Sets.newHashSet( planOperatorChildren ) );
                planOperatorChildren.forEach( child ->
                                              {
                                                  parentMap.put( child, planOperator );
                                                  planOperators.push( child );
                                              } );
                planDescriptionChildren.forEach( planDescriptions::push );
            }
            // traversing up. last time operator will be seen. compare operator and continue popping the stack
            else if ( parentMap.containsKey( planOperator ) && /*is not root*/
                      !childrenMap.get( parentMap.get( planOperator ) ).isEmpty() /*has not yet been visited on traversal UP tree*/ )
            {
                assertTrue( childrenMap.get( parentMap.get( planOperator ) ).remove( planOperator ),
                            ERROR_IN_TEST_CODE + "\n" +
                            "Was not the first time this plan was removed: " + planDescriptionName );

                actualVisitedPlans++;
                assertThat( planOperatorName, equalTo( planDescriptionName ) );
            }
            // this is a leaf operator, compare operators then begin popping the stack
            else
            {
                actualVisitedPlans++;
                assertThat( planOperatorName, equalTo( planDescriptionName ) );
            }
        }
        // sanity checks, to make sure test code is actually traversing and comparing the entire logical plan
        assertTrue( planDescriptions.isEmpty(), ERROR_IN_TEST_CODE );
        assertThat( ERROR_IN_TEST_CODE, actualVisitedPlans, equalTo( expectedPlanCount ) );
    }

    private static String fullNameFor( PlanOperator planOperator )
    {
        List<String> children = planOperator.children().stream().map( PlannerDescriptionIT::nameFor ).collect( toList() );
        return nameFor( planOperator ) + children;
    }

    private static String nameFor( PlanOperator planOperator )
    {
        ArrayList<String> identifiers = Lists.newArrayList( planOperator.identifiers() );
        Collections.sort( identifiers );
        return planOperator.operatorType() + "[" + planOperator.id() + "]" + identifiers;
    }

    private static <PLAN> int operatorPlanCountFor( PLAN rootPlanDescription, PlanOperator rootPlanOperator, PlanAccessors<PLAN> planAccessors )
    {
        Stack<PLAN> planDescriptions = new Stack<>();
        int descriptionCount = 0;
        planDescriptions.push( rootPlanDescription );
        while ( !planDescriptions.isEmpty() )
        {
            descriptionCount++;
            PLAN plan = planDescriptions.pop();
            planAccessors.getChildren( plan ).forEach( planDescriptions::push );
        }

        Stack<PlanOperator> planOperators = new Stack<>();
        int operatorCount = 0;
        planOperators.push( rootPlanOperator );
        while ( !planOperators.isEmpty() )
        {
            operatorCount++;
            PlanOperator plan = planOperators.pop();
            plan.children().forEach( planOperators::push );
        }

        assertThat( "Plan representations should be of equal size", descriptionCount, equalTo( operatorCount ) );

        return operatorCount;
    }

    private interface PlanAccessors<PLAN>
    {
        int getId( PLAN plan );

        String getFullName( PLAN plan );

        List<PLAN> getChildren( PLAN plan );
    }

    private static class EmbeddedPlanDescriptionAccessors implements PlanAccessors<ExecutionPlanDescription>
    {
        @Override
        public int getId( ExecutionPlanDescription plan )
        {
            return ((InternalPlanDescription) plan).id();
        }

        @Override
        public String getFullName( ExecutionPlanDescription plan )
        {
            List<String> children = plan.getChildren().stream().map( this::getDescriptiveNameFor ).collect( toList() );
            return getDescriptiveNameFor( plan ) + children;
        }

        private String getDescriptiveNameFor( ExecutionPlanDescription plan )
        {
            List<String> identifiers = Lists.newArrayList( plan.getIdentifiers() );
            Collections.sort( identifiers );
            return plan.getName() + "[" + getId( plan ) + "]" + identifiers;
        }

        @Override
        public List<ExecutionPlanDescription> getChildren( ExecutionPlanDescription plan )
        {
            return plan.getChildren();
        }
    }

    private static class DriverPlanAccessors implements PlanAccessors<Plan>
    {
        @Override
        public int getId( Plan plan )
        {
            return System.identityHashCode( plan );
        }

        @Override
        public String getFullName( Plan plan )
        {
            List<String> children = plan.children().stream().map( this::getDescriptiveNameFor ).collect( toList() );
            return getDescriptiveNameFor( plan ) + children;
        }

        private String getDescriptiveNameFor( Plan plan )
        {
            List<String> identifiers = Lists.newArrayList( plan.identifiers() );
            Collections.sort( identifiers );
            return plan.operatorType() + "[" + getId( plan ) + "]" + identifiers;
        }

        @Override
        public List<Plan> getChildren( Plan plan )
        {
            return (List<Plan>) plan.children();
        }
    }
}
