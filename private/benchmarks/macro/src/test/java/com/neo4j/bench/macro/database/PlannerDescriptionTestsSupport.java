/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.tool.macro.DeploymentMode;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.PlanOperator;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription;
import org.neo4j.driver.summary.Plan;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

class PlannerDescriptionTestsSupport
{

    private static final String ERROR_IN_TEST_CODE = "Error is probably in test code that traverses logical plan";

    static Jvm getJvm()
    {
        return Jvm.defaultJvmOrFail();
    }

    static Collection<Object[]> allWorkloads( DeploymentMode mode ) throws IOException
    {
        Path tempDirectory = Files.createTempDirectory( "resource" );
        try ( Resources resources = new Resources( tempDirectory ) )
        {
            return Workload.all( resources, mode ).stream().map( w -> new Object[]{w} ).collect( Collectors.toList() );
        }
        finally
        {
            BenchmarkUtil.deleteDir( tempDirectory );
        }
    }

    static <PLAN> void assertPlansEqual( PlanOperator rootPlanOperator, PLAN planDescriptionRoot, PlanAccessors<PLAN> planAccessors )
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
            assertFalse( ERROR_IN_TEST_CODE, planDescriptions.isEmpty() );

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
                assertTrue( ERROR_IN_TEST_CODE + "\n" +
                            "Was not the first time this plan was removed: " + planDescriptionName,
                            childrenMap.get( parentMap.get( planOperator ) ).remove( planOperator ) );

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
        assertTrue( ERROR_IN_TEST_CODE, planDescriptions.isEmpty() );
        assertThat( ERROR_IN_TEST_CODE, actualVisitedPlans, equalTo( expectedPlanCount ) );
    }

    private static String fullNameFor( PlanOperator planOperator )
    {
        List<String> children = planOperator.children().stream().map( PlannerDescriptionTestsSupport::nameFor ).collect( toList() );
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

    static class EmbeddedPlanDescriptionAccessors implements PlanAccessors<ExecutionPlanDescription>
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

    static class DriverPlanAccessors implements PlanAccessors<Plan>
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

    private final int httpPort = PortAuthority.allocatePort();
    private final int boltPort = PortAuthority.allocatePort();
    private final String randId = UUID.randomUUID().toString();
    private Path neo4jDir;

    void copyNeo4jDir( TestDirectory temporaryFolder ) throws IOException
    {
        Path originalDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
        this.neo4jDir = temporaryFolder.directory( format( "neo4jDir-%s", randId ) ).toPath();
        FileUtils.copyDirectory( originalDir.toFile(), neo4jDir.toFile() );
    }

    void deleteTemporaryNeo4j() throws IOException
    {
        FileUtils.deleteDirectory( neo4jDir.toFile() );
    }

    String randId()
    {
        return randId;
    }

    Path neo4jDir()
    {
        return neo4jDir;
    }

    Path writeEmbeddedNeo4jConfig()
    {
        Path neo4jConfigFile = neo4jDir().resolve( "conf" ).resolve( format( "neo4j-%s.conf", randId() ) );
        Neo4jConfigBuilder.withDefaults()
                          .writeToFile( neo4jConfigFile );
        return neo4jConfigFile;
    }

    Path writeServerNeo4jConfig( String name )
    {
        Path neo4jConfigFile = neo4jDir().resolve( "conf" ).resolve( format( "neo4j-%s-%s.conf", name, randId() ) );
        Neo4jConfigBuilder.withDefaults()
                          .withSetting( BoltConnector.listen_address, ":" + boltPort() )
                          .withSetting( HttpConnector.enabled, "true" )
                          .withSetting( HttpConnector.listen_address, ":" + httpPort() )
                          .writeToFile( neo4jConfigFile );
        return neo4jConfigFile;
    }

    int boltPort()
    {
        return boltPort;
    }

    int httpPort()
    {
        return httpPort;
    }
}
