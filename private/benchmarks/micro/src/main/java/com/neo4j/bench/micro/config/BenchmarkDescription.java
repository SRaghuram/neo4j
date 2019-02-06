/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.config;

import com.google.common.collect.Sets;
import com.neo4j.bench.micro.benchmarks.Kaboom;
import org.openjdk.jmh.annotations.BenchmarkMode;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

/**
 * Describes one available benchmark (class that extends AbstractBenchmark),
 * including name, group, description, enabled status, thread safety, methods, and parameters.
 */
public class BenchmarkDescription
{
    private final String className;
    private final String group;
    private final boolean isThreadSafe;
    private final Map<String,BenchmarkMethodDescription> methods;
    private final Map<String,BenchmarkParamDescription> parameters;
    private final String description;
    private boolean isEnabled;

    public BenchmarkDescription(
            String className,
            String group,
            boolean isThreadSafe,
            Map<String,BenchmarkMethodDescription> methods,
            Map<String,BenchmarkParamDescription> parameters,
            String description,
            boolean isEnabled )
    {
        this.className = className;
        this.group = group;
        this.isThreadSafe = isThreadSafe;
        this.methods = methods;
        this.parameters = parameters;
        this.description = description;
        this.isEnabled = isEnabled;
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public String className()
    {
        return className;
    }

    public String description()
    {
        return description;
    }

    public String group()
    {
        return group;
    }

    public boolean isThreadSafe()
    {
        return isThreadSafe;
    }

    public Map<String,BenchmarkParamDescription> parameters()
    {
        return parameters;
    }

    public Collection<BenchmarkMethodDescription> methods()
    {
        return methods.values();
    }

    String simpleName()
    {
        return className.substring( className.lastIndexOf( '.' ) + 1 );
    }

    public BenchmarkDescription copyRetainingMethods( String... methods )
    {
        Map<String,BenchmarkMethodDescription> retained = new HashMap<>();
        for ( String method : methods )
        {
            BenchmarkMethodDescription description = this.methods.get( method );
            retained.put( method, description );
        }
        return new BenchmarkDescription( className, group, isThreadSafe, retained, parameters, description, isEnabled );
    }

    public BenchmarkDescription copyAndEnable()
    {
        return new BenchmarkDescription(
                className,
                group,
                isThreadSafe,
                methods,
                parameters,
                description,
                true );
    }

    BenchmarkDescription copyWithConfig( BenchmarkConfigFileEntry benchmarkConfigEntry, Validation validation )
    {
        if ( !className.equals( benchmarkConfigEntry.name() ) )
        {
            throw new RuntimeException( format( "Cannot apply config for '%s' on benchmark '%s'",
                    benchmarkConfigEntry.name(),
                    className ) );
        }
        Map<String,BenchmarkParamDescription> newParameters = new HashMap<>( parameters );
        benchmarkConfigEntry.values().forEach( ( paramName, newValues ) ->
        {
            if ( !parameters.containsKey( paramName ) )
            {
                validation.configuredParameterDoesNotExist( className, paramName );
            }
            else
            {
                BenchmarkParamDescription oldValues = parameters.get( paramName );
                for ( String newValue : newValues )
                {
                    if ( !oldValues.allowedValues().contains( newValue ) )
                    {
                        validation.configuredValueIsNotAllowed(
                                className,
                                paramName,
                                oldValues.allowedValues(),
                                newValue );
                    }
                }
                newParameters.put( paramName, oldValues.withValues( newValues ) );
            }
        } );
        if ( benchmarkConfigEntry.isEnabled() )
        {
            newParameters.keySet().stream()
                    .filter( paramName ->
                            newParameters.get( paramName ).values().isEmpty() )
                    .forEach( paramName ->
                            validation.paramOfEnabledBenchmarkConfiguredWithNoValues( className, paramName ) );
        }
        return new BenchmarkDescription(
                className,
                group,
                isThreadSafe,
                methods,
                newParameters,
                description,
                benchmarkConfigEntry.isEnabled() );
    }

    public int executionCount( int threadCount )
    {
        if ( !isThreadSafe && threadCount != 1 )
        {
            return 0;
        }
        else
        {
            int parameterCombinations = 1;
            for ( BenchmarkParamDescription param : parameters.values() )
            {
                parameterCombinations *= param.values().size();
            }
            int executionCount = 0;
            for ( BenchmarkMethodDescription method : methods() )
            {
                executionCount += parameterCombinations * method.modes().size();
            }
            return executionCount;
        }
    }

    public int storeCount( List<String> contributingParameters )
    {
        int parameterCombinations = 1;
        for ( BenchmarkParamDescription param : parameters.values() )
        {
            if ( contributingParameters.contains( param.name() ) )
            {
                parameterCombinations *= param.values().size();
            }
        }
        return parameterCombinations;
    }

    static Set<Set<BenchmarkParamDescription>> explodeParameters( List<BenchmarkParamDescription> parameters )
    {
        if ( parameters.isEmpty() )
        {
            throw new Kaboom( "This should never happen. Base case is when collection contains one element" );
        }
        else if ( parameters.size() == 1 )
        {
            return parameters.get( 0 ).explode().stream()
                    .map( Sets::newHashSet )
                    .collect( toSet() );
        }
        else
        {
            BenchmarkParamDescription head = parameters.get( 0 );
            List<BenchmarkParamDescription> tail = parameters.subList( 1, parameters.size() );

            Set<Set<BenchmarkParamDescription>> exploded = new HashSet<>();
            Set<Set<BenchmarkParamDescription>> explodedTail = explodeParameters( tail );

            for ( BenchmarkParamDescription headElement : head.explode() )
            {
                for ( Set<BenchmarkParamDescription> explodedTailElement : explodedTail )
                {
                    Set<BenchmarkParamDescription> explodedElement = new HashSet<>();
                    explodedElement.addAll( explodedTailElement );
                    explodedElement.add( headElement );
                    exploded.add( explodedElement );
                }
            }
            return exploded;
        }
    }

    public Set<BenchmarkDescription> explode()
    {
        Set<BenchmarkDescription> allCombinations = new HashSet<>();

        for ( BenchmarkMethodDescription method : methods.values().stream()
                .flatMap( method -> method.explode().stream() )
                .collect( toList() ) )
        {
            if ( parameters.isEmpty() )
            {
                HashMap<String,BenchmarkMethodDescription> methodForCombination = new HashMap<>();
                methodForCombination.put( method.name(), method );
                Map<String,BenchmarkParamDescription> emptyParameters = new HashMap<>();
                allCombinations.add(
                        new BenchmarkDescription(
                                className,
                                group,
                                isThreadSafe,
                                methodForCombination,
                                emptyParameters,
                                description,
                                isEnabled ) );
            }
            else
            {
                for ( Set<BenchmarkParamDescription> parameterCombination :
                        explodeParameters( newArrayList( parameters.values() ) ) )
                {
                    HashMap<String,BenchmarkMethodDescription> methodForCombination = new HashMap<>();
                    methodForCombination.put( method.name(), method );
                    Map<String,BenchmarkParamDescription> parametersForCombination = parameterCombination.stream()
                            .collect( toMap( BenchmarkParamDescription::name, p -> p ) );

                    allCombinations.add(
                            new BenchmarkDescription(
                                    className,
                                    group,
                                    isThreadSafe,
                                    methodForCombination,
                                    parametersForCombination,
                                    description,
                                    isEnabled ) );
                }
            }
        }
        return allCombinations;
    }

    @Override
    public String toString()
    {
        return format( "%s,\n" +
                       "\tenabled=%b,\n" +
                       "\tthread safe=%b,\n" +
                       "\tmethods=%s,\n" +
                       "\tparams=%s\n", className, isEnabled, isThreadSafe, methods, parameters );
    }

    // FACTORY METHODS

    public static BenchmarkDescription of( Class clazz, Validation validation )
    {
        Map<String,BenchmarkParamDescription> parameters = Annotations.paramFieldsFor( clazz )
                .collect( toMap(
                        field -> simplifyParamName( field.getName() ),
                        field ->
                        {
                            String paramName = simplifyParamName( field.getName() );
                            ParamValues paramValues = field.getAnnotation( ParamValues.class );

                            Set<String> allowedValues = newHashSet( paramValues.allowed() );
                            if ( allowedValues.size() != paramValues.allowed().length )
                            {
                                validation.duplicateAllowedValue( clazz.getName(), paramName, paramValues.allowed() );
                            }

                            Set<String> defaultValues = newHashSet( paramValues.base() );
                            if ( defaultValues.size() != paramValues.base().length )
                            {
                                validation.duplicateBaseValue( clazz.getName(), paramName, paramValues.base() );
                            }
                            return new BenchmarkParamDescription( paramName, allowedValues, defaultValues );
                        } ) );
        Map<String,BenchmarkMethodDescription> methods = Annotations.benchmarkMethodsFor( clazz )
                .map( benchmarkMethod ->
                        new BenchmarkMethodDescription(
                                Annotations.benchmarkNameFor( benchmarkMethod ),
                                benchmarkMethod.getAnnotation( BenchmarkMode.class ).value()
                        ) )
                // distinct is needed because @Group benchmarks have multiple methods all with same name
                // distinct is safe because every method of the same group should have the same mode
                .distinct()
                .collect( toMap( BenchmarkMethodDescription::name, identity() ) );
        return new BenchmarkDescription(
                clazz.getName(),
                Annotations.benchmarkGroupFor( clazz ),
                Annotations.isThreadSafe( clazz ),
                methods,
                parameters,
                Annotations.descriptionFor( clazz ),
                Annotations.isEnabled( clazz ) );
    }

    // HELPERS

    public static String simplifyParamName( String jmhParamName )
    {
        if ( !jmhParamName.contains( "_" ) )
        {
            throw new RuntimeException( "Invalid JMH @param name, does not contain '_' character :" + jmhParamName );
        }
        return jmhParamName.substring( jmhParamName.indexOf( "_" ) + 1, jmhParamName.length() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkDescription that = (BenchmarkDescription) o;
        return isThreadSafe == that.isThreadSafe &&
               isEnabled == that.isEnabled &&
               Objects.equals( className, that.className ) &&
               Objects.equals( group, that.group ) &&
               Objects.equals( methods, that.methods ) &&
               Objects.equals( parameters, that.parameters ) &&
               Objects.equals( description, that.description );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( className, group, isThreadSafe, methods, parameters, description, isEnabled );
    }
}
