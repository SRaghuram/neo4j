/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.google.common.collect.Lists;
import com.neo4j.bench.model.model.Plan;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.model.PlanTree;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import org.neo4j.cypher.internal.plandescription.Argument;
import org.neo4j.cypher.internal.plandescription.ArgumentPlanDescription;
import org.neo4j.cypher.internal.plandescription.Arguments;
import org.neo4j.cypher.internal.plandescription.Children;
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription;
import org.neo4j.cypher.internal.plandescription.NoChildren$;
import org.neo4j.cypher.internal.plandescription.PlanDescriptionImpl;
import org.neo4j.cypher.internal.plandescription.PrettyString;
import org.neo4j.cypher.internal.plandescription.PrettyString$;
import org.neo4j.cypher.internal.plandescription.SingleChild;
import org.neo4j.cypher.internal.plandescription.TwoChildren;
import org.neo4j.cypher.internal.plandescription.renderAsTreeTable;
import org.neo4j.graphdb.ExecutionPlanDescription;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class PlannerDescription
{
    private static final String UNKNOWN = "unknown";
    private static final String PLANNER_IMPL = "planner-impl";
    private static final String RUNTIME_IMPL = "runtime-impl";
    private static final String DB_HITS = "DbHits";
    private static final String VERSION = "version";
    private static final String ESTIMATED_ROWS = "EstimatedRows";
    private static final String ROWS = "Rows";
    private static final DriverPlanExtractor DRIVER_PLAN_EXTRACTOR = new DriverPlanExtractor();
    private static final EmbeddedPlanExtractor EMBEDDED_PLAN_EXTRACTOR = new EmbeddedPlanExtractor();

    public static String toAsciiPlan( PlanOperator plan )
    {
        InternalPlanDescription planDescription = toPlanDescription( plan );
        return renderAsTreeTable.apply( planDescription );
    }

    private static InternalPlanDescription toPlanDescription( PlanOperator plan )
    {
        int id = plan.id();
        String name = plan.operatorType();

        List<Argument> javaArguments = new ArrayList<>();

        javaArguments.add( Arguments.EstimatedRows$.MODULE$.apply( plan.estimatedRows().doubleValue() ) );
        plan.rows().ifPresent( rowsValue -> javaArguments.add( Arguments.Rows$.MODULE$.apply( rowsValue ) ) );

        List<PrettyString> prettyStrings = plan.detailStrings().stream()
                                               .map( PrettyString$.MODULE$::apply )
                                               .collect( toList() );
        Arguments.Details details = Arguments.Details$.MODULE$.apply( JavaConverters.iterableAsScalaIterable( prettyStrings ).toSeq() );
        javaArguments.add( details );

        Seq<Argument> arguments = JavaConverters.iterableAsScalaIterable( javaArguments ).toSeq();

        scala.collection.immutable.Set<PrettyString> variables = JavaConverters.asScalaSetConverter( Collections.<String>emptySet() ).asScala().toSet();

        if ( name.equals( "Argument" ) || name.equals( "EmptyRow" ) )
        {
            return new ArgumentPlanDescription( id, arguments, variables );
        }
        else
        {
            Children children = getChildren( plan );
            return new PlanDescriptionImpl( id, name, children, arguments, variables );
        }
    }

    private static Children getChildren( PlanOperator plan )
    {
        switch ( plan.children().size() )
        {
        case 0:
            return NoChildren$.MODULE$;
        case 1:
            return new SingleChild( toPlanDescription( plan.children().get( 0 ) ) );
        case 2:
            return new TwoChildren( toPlanDescription( plan.children().get( 0 ) ), toPlanDescription( plan.children().get( 1 ) ) );
        default:
            throw new IllegalStateException( format( "Plan '%s' had %s children, but no operator should have more than two",
                                                     plan.operatorType(),
                                                     plan.children().size() ) );
        }
    }

    public static PlannerDescription fromResults( ExecutionPlanDescription profileWithPlannerAndRuntime,
                                                  ExecutionPlanDescription explainWithoutPlannerOrRuntime,
                                                  String requestedPlanner,
                                                  String requestedRuntime )
    {
        Map<String,Object> usedArguments = profileWithPlannerAndRuntime.getArguments();
        String usedPlanner = (String) usedArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String usedRuntime = (String) usedArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );
        Long dbHits = (Long) usedArguments.get( DB_HITS );

        Map<String,Object> defaultArguments = explainWithoutPlannerOrRuntime.getArguments();
        String defaultPlanner = (String) defaultArguments.getOrDefault( PLANNER_IMPL, UNKNOWN );
        String defaultRuntime = (String) defaultArguments.getOrDefault( RUNTIME_IMPL, UNKNOWN );

        return new PlannerDescription( requestedPlanner.toLowerCase(),
                                       usedPlanner.toLowerCase(),
                                       defaultPlanner.toLowerCase(),
                                       requestedRuntime,
                                       usedRuntime.toLowerCase(),
                                       defaultRuntime.toLowerCase(),
                                       profileWithPlannerAndRuntime,
                                       dbHits );
    }

    private final String requestedPlanner;
    private final String usedPlanner;
    private final String defaultPlanner;
    private final String requestedRuntime;
    private final String usedRuntime;
    private final String defaultRuntime;
    private final ExecutionPlanDescription planDescription;
    private final Long dbHits;

    private PlannerDescription( String requestedPlanner,
                                String usedPlanner,
                                String defaultPlanner,
                                String requestedRuntime,
                                String usedRuntime,
                                String defaultRuntime,
                                ExecutionPlanDescription planDescription,
                                Long dbHits )
    {
        this.requestedPlanner = requestedPlanner;
        this.usedPlanner = usedPlanner;
        this.defaultPlanner = defaultPlanner;
        this.requestedRuntime = requestedRuntime;
        this.usedRuntime = usedRuntime;
        this.defaultRuntime = defaultRuntime;
        this.planDescription = planDescription;
        this.dbHits = dbHits;
    }

    public String asciiPlanDescription()
    {
        return planDescription.toString();
    }

    public String requestedPlanner()
    {
        return requestedPlanner;
    }

    public String usedPlanner()
    {
        return usedPlanner;
    }

    public String defaultPlanner()
    {
        return defaultPlanner;
    }

    public String requestedRuntime()
    {
        return requestedRuntime;
    }

    public String usedRuntime()
    {
        return usedRuntime;
    }

    public String defaultRuntime()
    {
        return defaultRuntime;
    }

    public ExecutionPlanDescription planDescription()
    {
        return planDescription;
    }

    public Long dbHits()
    {
        return dbHits;
    }

    public Plan toPlan()
    {
        String version = (String) planDescription.getArguments().get( VERSION );
        return new Plan(
                requestedPlanner,
                usedPlanner,
                defaultPlanner,
                requestedRuntime,
                usedRuntime,
                defaultRuntime,
                version,
                new PlanTree(
                        asciiPlanDescription(),
                        toPlanOperator( planDescription ) )
        );
    }

    // This is necessary when PLAN is org.neo4j.driver.summary.Plan, as it has no field that is guaranteed unique between plans
    private static class HashCodePlanWrapper<PLAN>
    {
        private final PLAN plan;

        private HashCodePlanWrapper( PLAN plan )
        {
            this.plan = plan;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode( plan );
        }

        @Override
        public boolean equals( Object obj )
        {
            return plan.equals( ((HashCodePlanWrapper)obj).plan );
        }
    }

    private abstract static class PlanExtractor<PLAN>
    {
        protected abstract int getId( PLAN plan );

        protected abstract String getName( PLAN plan );

        protected abstract List<PLAN> getChildren( PLAN plan );

        protected abstract Optional<Long> getRows( PLAN plan );

        protected abstract long getEstimatedRows( PLAN plan );

        protected abstract Optional<Long> getDbHits( PLAN plan );

        protected abstract List<String> getIdentifiers( PLAN plan );

        protected abstract List<String> getDetails( PLAN plan );

        PlanOperator toPlanOperator( PLAN root )
        {
            Map<HashCodePlanWrapper<PLAN>,HashCodePlanWrapper<PLAN>> inputPlanParentMap = new HashMap<>();
            Map<HashCodePlanWrapper<PLAN>,List<PlanOperator>> childrenMap = new HashMap<>();
            Stack<HashCodePlanWrapper<PLAN>> inputPlanStack = new Stack<>();

            // initialization
            inputPlanStack.push( new HashCodePlanWrapper<>( root ) );
            childrenMap.put( new HashCodePlanWrapper<>( root ), new ArrayList<>() );

            while ( !inputPlanStack.isEmpty() )
            {
                HashCodePlanWrapper<PLAN> inputPlan = inputPlanStack.pop();
                List<PLAN> childrenDescriptions = getChildren( inputPlan.plan );
                List<PlanOperator> childOperators = childrenMap.get( inputPlan );
                // this is a leaf plan _OR_ all leaves of this plan have already been processed
                if ( childrenDescriptions.isEmpty() || !childOperators.isEmpty() )
                {
                    PlanOperator planOperator = toPlanOperator( inputPlan.plan, childOperators );
                    HashCodePlanWrapper<PLAN> inputPlanParent = inputPlanParentMap.get( inputPlan );
                    // if is not root
                    if ( null != inputPlanParent )
                    {
                        // add processed plan to children of parent
                        childrenMap.get( inputPlanParent ).add( planOperator );
                    }
                }
                else
                {
                    inputPlanStack.push( inputPlan );
                    // push RHS first, so LHS is popped first
                    Lists.reverse( childrenDescriptions ).forEach( child ->
                                                                   {
                                                                       HashCodePlanWrapper<PLAN> childWithHashCode = new HashCodePlanWrapper<>( child );
                                                                       inputPlanParentMap.put( childWithHashCode, inputPlan );
                                                                       childrenMap.put( childWithHashCode, new ArrayList<>() );
                                                                       inputPlanStack.push( childWithHashCode );
                                                                   } );
                }
            }

            return toPlanOperator( root, childrenMap.get( new HashCodePlanWrapper<>( root ) ) );
        }

        private PlanOperator toPlanOperator( PLAN inputPlan, List<PlanOperator> children )
        {
            String operatorType = getName( inputPlan );
            Long estimatedRows = getEstimatedRows( inputPlan );
            Optional<Long> dbHits = getDbHits( inputPlan );
            Optional<Long> rows = getRows( inputPlan );

            List<String> identifiers = getIdentifiers( inputPlan );

            List<String> detailStrings = getDetails( inputPlan );

            int id = getId( inputPlan );

            return new PlanOperator( id, operatorType, estimatedRows, dbHits.orElse( null ), rows.orElse( null ), identifiers, children, detailStrings );
        }
    }

    private static class EmbeddedPlanExtractor extends PlanExtractor<ExecutionPlanDescription>
    {
        @Override
        protected int getId( ExecutionPlanDescription executionPlanDescription )
        {
            if ( executionPlanDescription instanceof InternalPlanDescription )
            {
                return ((InternalPlanDescription) executionPlanDescription).id();
            }
            else
            {
                // Alternatively, this method could fallback to System::identityHashCode which is enough for our current tests to pass
                // System.identityHashCode( executionPlanDescription );
                throw new RuntimeException( "Plan description does not have an id\n" + executionPlanDescription );
            }
        }

        @Override
        protected String getName( ExecutionPlanDescription executionPlanDescription )
        {
            return executionPlanDescription.getName();
        }

        @Override
        protected List<ExecutionPlanDescription> getChildren( ExecutionPlanDescription executionPlanDescription )
        {
            return executionPlanDescription.getChildren();
        }

        @Override
        protected Optional<Long> getRows( ExecutionPlanDescription executionPlanDescription )
        {
            // some queries are run with 'explain' instead of 'profile', e.g., those with PERIODIC COMMIT
            return executionPlanDescription.hasProfilerStatistics()
                   ? Optional.of( executionPlanDescription.getProfilerStatistics().getRows() )
                   : Optional.empty();
        }

        @Override
        protected long getEstimatedRows( ExecutionPlanDescription executionPlanDescription )
        {
            return ((Number) executionPlanDescription.getArguments().get( ESTIMATED_ROWS )).longValue();
        }

        @Override
        protected Optional<Long> getDbHits( ExecutionPlanDescription executionPlanDescription )
        {
            // some queries are run with 'explain' instead of 'profile', e.g., those with PERIODIC COMMIT
            return executionPlanDescription.hasProfilerStatistics()
                   ? Optional.of( executionPlanDescription.getProfilerStatistics().getDbHits() )
                   : Optional.empty();
        }

        @Override
        protected List<String> getIdentifiers( ExecutionPlanDescription executionPlanDescription )
        {
            return Lists.newArrayList( executionPlanDescription.getIdentifiers() );
        }

        @Override
        protected List<String> getDetails( ExecutionPlanDescription executionPlanDescription )
        {
            return JavaConverters.asJavaCollection( ((InternalPlanDescription) executionPlanDescription).arguments() )
                                 .stream()
                                 .filter( argument -> argument instanceof Arguments.Details )
                                 .map( argument -> (Arguments.Details) argument )
                                 .flatMap( details -> JavaConverters.asJavaCollection( details.info() ).stream() )
                                 .map( PrettyString::prettifiedString )
                                 .collect( toList() );
        }
    }

    private static class DriverPlanExtractor extends PlanExtractor<org.neo4j.driver.summary.Plan>
    {
        @Override
        protected int getId( org.neo4j.driver.summary.Plan plan )
        {
            return System.identityHashCode( plan );
        }

        @Override
        protected String getName( org.neo4j.driver.summary.Plan plan )
        {
            return plan.operatorType();
        }

        @Override
        protected List<org.neo4j.driver.summary.Plan> getChildren( org.neo4j.driver.summary.Plan plan )
        {
            return (List<org.neo4j.driver.summary.Plan>) plan.children();
        }

        @Override
        protected Optional<Long> getRows( org.neo4j.driver.summary.Plan plan )
        {
            return plan.arguments().containsKey( ROWS )
                   ? Optional.of( plan.arguments().get( ROWS ).asNumber().longValue() )
                   : Optional.empty();
        }

        @Override
        protected long getEstimatedRows( org.neo4j.driver.summary.Plan plan )
        {
            return plan.arguments().get( ESTIMATED_ROWS ).asNumber().longValue();
        }

        @Override
        protected Optional<Long> getDbHits( org.neo4j.driver.summary.Plan plan )
        {
            return plan.arguments().containsKey( DB_HITS )
                   ? Optional.of( plan.arguments().get( DB_HITS ).asNumber().longValue() )
                   : Optional.empty();
        }

        @Override
        protected List<String> getIdentifiers( org.neo4j.driver.summary.Plan plan )
        {
            return Lists.newArrayList( plan.identifiers() );
        }

        @Override
        protected List<String> getDetails( org.neo4j.driver.summary.Plan plan )
        {
            return plan.arguments().containsKey( "Details" )
                   ? Arrays.asList( plan.arguments().get( "Details" ).asString() )
                   : Collections.emptyList();
        }
    }

    public static PlanOperator toPlanOperator( org.neo4j.driver.summary.Plan root )
    {
        return DRIVER_PLAN_EXTRACTOR.toPlanOperator( root );
    }

    public static PlanOperator toPlanOperator( ExecutionPlanDescription root )
    {
        return EMBEDDED_PLAN_EXTRACTOR.toPlanOperator( root );
    }
}
