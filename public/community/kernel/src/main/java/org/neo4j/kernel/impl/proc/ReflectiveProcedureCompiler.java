/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.proc;

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.FailedLoadAggregatedFunction;
import org.neo4j.kernel.api.proc.FailedLoadFunction;
import org.neo4j.kernel.api.proc.FailedLoadProcedure;
import org.neo4j.kernel.impl.proc.OutputMappers.OutputMapper;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.procedure_unrestricted;

/**
 * Handles converting a class into one or more callable {@link CallableProcedure}.
 */
class ReflectiveProcedureCompiler
{
    private final MethodHandles.Lookup lookup = MethodHandles.lookup();
    private final OutputMappers outputMappers;
    private final MethodSignatureCompiler inputSignatureDeterminer;
    private final FieldInjections safeFieldInjections;
    private final FieldInjections allFieldInjections;
    private final Log log;
    private final TypeMappers typeMappers;
    private final ProcedureConfig config;
    private final NamingRestrictions restrictions;

    ReflectiveProcedureCompiler( TypeMappers typeMappers, ComponentRegistry safeComponents,
            ComponentRegistry allComponents, Log log, ProcedureConfig config )
    {
        this(
                new MethodSignatureCompiler( typeMappers ),
                new OutputMappers( typeMappers ),
                new FieldInjections( safeComponents ),
                new FieldInjections( allComponents ),
                log,
                typeMappers,
                config,
                ReflectiveProcedureCompiler::rejectEmptyNamespace );
    }

    private ReflectiveProcedureCompiler(
            MethodSignatureCompiler inputSignatureCompiler,
            OutputMappers outputMappers,
            FieldInjections safeFieldInjections,
            FieldInjections allFieldInjections,
            Log log,
            TypeMappers typeMappers,
            ProcedureConfig config,
            NamingRestrictions restrictions )
    {
        this.inputSignatureDeterminer = inputSignatureCompiler;
        this.outputMappers = outputMappers;
        this.safeFieldInjections = safeFieldInjections;
        this.allFieldInjections = allFieldInjections;
        this.log = log;
        this.typeMappers = typeMappers;
        this.config = config;
        this.restrictions = restrictions;
    }

    List<CallableUserFunction> compileFunction( Class<?> fcnDefinition ) throws KernelException
    {
        try
        {
            List<Method> functionMethods = Arrays.stream( fcnDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( UserFunction.class ) )
                    .collect( Collectors.toList() );

            if ( functionMethods.isEmpty() )
            {
                return emptyList();
            }

            //used for proper error handling
            assertValidConstructor( fcnDefinition );

            ArrayList<CallableUserFunction> out = new ArrayList<>( functionMethods.size() );
            for ( Method method : functionMethods )
            {
                String valueName = method.getAnnotation( UserFunction.class ).value();
                String definedName = method.getAnnotation( UserFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );
                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileFunction( fcnDefinition, method, funcName ) );
                }
                else
                {
                    log.warn( String.format( "The function '%s' is not on the whitelist and won't be loaded.",
                            funcName.toString() ) );
                }
            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
            return out;
        }
        catch ( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile function defined in `%s`: %s", fcnDefinition.getSimpleName(), e.getMessage() );
        }
    }

    List<CallableUserAggregationFunction> compileAggregationFunction( Class<?> fcnDefinition ) throws KernelException
    {
        try
        {
            List<Method> methods = Arrays.stream( fcnDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( UserAggregationFunction.class ) )
                    .collect( Collectors.toList() );

            if ( methods.isEmpty() )
            {
                return emptyList();
            }

            MethodHandle constructor = assertValidConstructor( fcnDefinition );

            ArrayList<CallableUserAggregationFunction> out = new ArrayList<>( methods.size() );
            for ( Method method : methods )
            {
                String valueName = method.getAnnotation( UserAggregationFunction.class ).value();
                String definedName = method.getAnnotation( UserAggregationFunction.class ).name();
                QualifiedName funcName = extractName( fcnDefinition, method, valueName, definedName );

                if ( config.isWhitelisted( funcName.toString() ) )
                {
                    out.add( compileAggregationFunction( fcnDefinition, constructor, method, funcName ) );
                }
                else
                {
                    log.warn( String.format( "The function '%s' is not on the whitelist and won't be loaded.",
                            funcName.toString() ) );
                }

            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
            return out;
        }
        catch ( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile function defined in `%s`: %s", fcnDefinition.getSimpleName(), e.getMessage() );
        }
    }

    List<CallableProcedure> compileProcedure( Class<?> procDefinition, String warning, boolean fullAccess )
            throws KernelException
    {
        try
        {
            List<Method> procedureMethods = Arrays.stream( procDefinition.getDeclaredMethods() )
                    .filter( m -> m.isAnnotationPresent( Procedure.class ) )
                    .collect( Collectors.toList() );

            if ( procedureMethods.isEmpty() )
            {
                return emptyList();
            }

            assertValidConstructor( procDefinition );
            ArrayList<CallableProcedure> out = new ArrayList<>( procedureMethods.size() );
            for ( Method method : procedureMethods )
            {
                String valueName = method.getAnnotation( Procedure.class ).value();
                String definedName = method.getAnnotation( Procedure.class ).name();
                QualifiedName procName = extractName( procDefinition, method, valueName, definedName );

                if ( fullAccess || config.isWhitelisted( procName.toString() ) )
                {
                    out.add( compileProcedure( procDefinition, method, warning, fullAccess, procName ) );
                }
                else
                {
                    log.warn( String.format( "The procedure '%s' is not on the whitelist and won't be loaded.",
                            procName.toString() ) );
                }
            }
            out.sort( Comparator.comparing( a -> a.signature().name().toString() ) );
            return out;
        }
        catch ( KernelException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile procedure defined in `%s`: %s", procDefinition.getSimpleName(), e.getMessage() );
        }
    }

    private CallableProcedure compileProcedure( Class<?> procDefinition, Method method,
            String warning, boolean fullAccess, QualifiedName procName  )
            throws ProcedureException
    {
        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        OutputMapper outputMapper = outputMappers.mapper( method );

        String description = description( method );
        Procedure procedure = method.getAnnotation( Procedure.class );
        Mode mode = procedure.mode();
        boolean admin = method.isAnnotationPresent( Admin.class );
        String deprecated = deprecated( method, procedure::deprecatedBy,
                "Use of @Procedure(deprecatedBy) without @Deprecated in " + procName );

        List<FieldSetter> setters = allFieldInjections.setters( procDefinition );
        if ( !fullAccess && !config.fullAccessFor( procName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( procDefinition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( procName );
                ProcedureSignature signature =
                        new ProcedureSignature( procName, inputSignature, outputMapper.signature(), Mode.DEFAULT,
                                admin, null, new String[0], description, warning, procedure.eager(), false );
                return new FailedLoadProcedure( signature );
            }
        }

        ProcedureSignature signature =
                new ProcedureSignature( procName, inputSignature, outputMapper.signature(), mode, admin, deprecated,
                        config.rolesFor( procName.toString() ), description, warning, procedure.eager(), false );

        return ProcedureCompilation.compileProcedure( signature, setters, method );
    }

    private String describeAndLogLoadFailure( QualifiedName name )
    {
        String nameStr = name.toString();
        String description =
                nameStr + " is unavailable because it is sandboxed and has dependencies outside of the sandbox. " +
                "Sandboxing is controlled by the " + procedure_unrestricted.name() + " setting. " +
                "Only unrestrict procedures you can trust with access to database internals.";
        log.warn( description );
        return description;
    }

    private CallableUserFunction compileFunction( Class<?> procDefinition, Method method, QualifiedName procName )
            throws ProcedureException
    {
        restrictions.verify( procName );

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( method );
        Class<?> returnType = method.getReturnType();
        TypeMappers.TypeChecker typeChecker = typeMappers.checkerFor( returnType );
        String description = description( method );
        UserFunction function = method.getAnnotation( UserFunction.class );
        String deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserFunction(deprecatedBy) without @Deprecated in " + procName );

        List<FieldSetter> setters = allFieldInjections.setters( procDefinition );
        if ( !config.fullAccessFor( procName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( procDefinition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( procName );
                UserFunctionSignature signature =
                        new UserFunctionSignature( procName, inputSignature, typeChecker.type(), deprecated,
                                config.rolesFor( procName.toString() ), description, false );
                return new FailedLoadFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( procName, inputSignature, typeChecker.type(), deprecated,
                        config.rolesFor( procName.toString() ), description, false );

        return ProcedureCompilation.compileFunction( signature, setters, method );
    }

    private CallableUserAggregationFunction compileAggregationFunction( Class<?> definition, MethodHandle constructor,
            Method method, QualifiedName funcName ) throws ProcedureException, IllegalAccessException
    {
        restrictions.verify( funcName );

        //find update and result method
        Method update = null;
        Method result = null;
        Class<?> aggregator = method.getReturnType();
        for ( Method m : aggregator.getDeclaredMethods() )
        {
            if ( m.isAnnotationPresent( UserAggregationUpdate.class ) )
            {
                if ( update != null )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Class '%s' contains multiple methods annotated with '@%s'.", aggregator.getSimpleName(),
                            UserAggregationUpdate.class.getSimpleName() );
                }
                update = m;

            }
            if ( m.isAnnotationPresent( UserAggregationResult.class ) )
            {
                if ( result != null )
                {
                    throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                            "Class '%s' contains multiple methods annotated with '@%s'.", aggregator.getSimpleName(),
                            UserAggregationResult.class.getSimpleName() );
                }
                result = m;
            }
        }
        if ( result == null || update == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Class '%s' must contain methods annotated with both '@%s' as well as '@%s'.",
                    aggregator.getSimpleName(), UserAggregationResult.class.getSimpleName(),
                    UserAggregationUpdate.class.getSimpleName() );
        }
        if ( update.getReturnType() != void.class )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Update method '%s' in %s has type '%s' but must have return type 'void'.", update.getName(),
                    aggregator.getSimpleName(), update.getReturnType().getSimpleName() );

        }
        if ( !Modifier.isPublic( method.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation method '%s' in %s must be public.", method.getName(), definition.getSimpleName() );
        }
        if ( !Modifier.isPublic( aggregator.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation class '%s' must be public.", aggregator.getSimpleName() );
        }
        if ( !Modifier.isPublic( update.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation update method '%s' in %s must be public.", update.getName(),
                    aggregator.getSimpleName() );
        }
        if ( !Modifier.isPublic( result.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Aggregation result method '%s' in %s must be public.", result.getName(),
                    aggregator.getSimpleName() );
        }

        List<FieldSignature> inputSignature = inputSignatureDeterminer.signatureFor( update );
        Class<?> returnType = result.getReturnType();
        TypeMappers.TypeChecker valueConverter = typeMappers.checkerFor( returnType );
        MethodHandle creator = lookup.unreflect( method );
        MethodHandle resultMethod = lookup.unreflect( result );

        String description = description( method );
        UserAggregationFunction function = method.getAnnotation( UserAggregationFunction.class );

        String deprecated = deprecated( method, function::deprecatedBy,
                "Use of @UserAggregationFunction(deprecatedBy) without @Deprecated in " + funcName );

        List<FieldSetter> setters = allFieldInjections.setters( definition );
        if ( !config.fullAccessFor( funcName.toString() ) )
        {
            try
            {
                setters = safeFieldInjections.setters( definition );
            }
            catch ( ComponentInjectionException e )
            {
                description = describeAndLogLoadFailure( funcName );
                UserFunctionSignature signature =
                        new UserFunctionSignature( funcName, inputSignature, valueConverter.type(), deprecated,
                                config.rolesFor( funcName.toString() ), description, false );

                return new FailedLoadAggregatedFunction( signature );
            }
        }

        UserFunctionSignature signature =
                new UserFunctionSignature( funcName, inputSignature, valueConverter.type(), deprecated,
                        config.rolesFor( funcName.toString() ), description, false );

        return new ReflectiveUserAggregationFunction( signature, constructor, creator, update, resultMethod,
                valueConverter, setters );
    }

    private String deprecated( Method method, Supplier<String> supplier, String warning )
    {
        String deprecatedBy = supplier.get();
        String deprecated = null;
        if ( method.isAnnotationPresent( Deprecated.class ) )
        {
            deprecated = deprecatedBy ;
        }
        else if ( !deprecatedBy.isEmpty() )
        {
            log.warn( warning );
            deprecated = deprecatedBy;
        }

        return deprecated;
    }

    private String description( Method method )
    {
        if ( method.isAnnotationPresent( Description.class ) )
        {
           return method.getAnnotation( Description.class ).value();
        }
        else
        {
            return null;
        }
    }

    private MethodHandle assertValidConstructor( Class<?> procDefinition ) throws ProcedureException
    {
        try
        {
            return lookup.unreflectConstructor( procDefinition.getConstructor() );
        }
        catch ( IllegalAccessException | NoSuchMethodException e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Unable to find a usable public no-argument assertValidConstructor in the class `%s`. " +
                    "Please add a valid, public assertValidConstructor, recompile the class and try again.",
                    procDefinition.getSimpleName() );
        }
    }

    private QualifiedName extractName( Class<?> procDefinition, Method m, String valueName, String definedName )
    {
        String procName = definedName.trim().isEmpty() ? valueName : definedName;
        if ( procName.trim().length() > 0 )
        {
            String[] split = procName.split( "\\." );
            if ( split.length == 1 )
            {
                return new QualifiedName( new String[0], split[0] );
            }
            else
            {
                int lastElement = split.length - 1;
                return new QualifiedName( Arrays.copyOf( split, lastElement ), split[lastElement] );
            }
        }
        Package pkg = procDefinition.getPackage();
        // Package is null if class is in root package
        String[] namespace = pkg == null ? new String[0] : pkg.getName().split( "\\." );
        String name = m.getName();
        return new QualifiedName( namespace, name );
    }

    public ReflectiveProcedureCompiler withoutNamingRestrictions()
    {
        return new ReflectiveProcedureCompiler(
                inputSignatureDeterminer,
                outputMappers,
                safeFieldInjections,
                allFieldInjections,
                log,
                typeMappers,
                config,
                name ->
                {
                    // all ok
                } );
    }

    private abstract static class ReflectiveBase
    {

        final List<FieldSetter> fieldSetters;

        ReflectiveBase( List<FieldSetter> fieldSetters )
        {
            this.fieldSetters = fieldSetters;
        }

        protected void inject( Context ctx, Object object ) throws ProcedureException
        {
            for ( FieldSetter setter : fieldSetters )
            {
                setter.apply( ctx, object );
            }
        }

        protected Object[] mapToObjects( String type, QualifiedName name, ValueMapper<Object> mapper, List<FieldSignature> inputSignature,
                AnyValue[] input ) throws ProcedureException
        {
            // Verify that the number of passed arguments matches the number expected in the mthod signature
            if ( inputSignature.size() != input.length )
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                        "%s `%s` takes %d arguments but %d was provided.", type, name,
                        inputSignature.size(), input.length );
            }

            Object[] args = new Object[input.length];
            for ( int i = 0; i < input.length; i++ )
            {
                args[i] = inputSignature.get( i ).map( input[i], mapper );
            }
            return args;
        }
    }

    private static class ReflectiveUserAggregationFunction extends ReflectiveBase implements
            CallableUserAggregationFunction
    {

        private final TypeMappers.TypeChecker typeChecker;
        private final UserFunctionSignature signature;
        private final MethodHandle constructor;
        private final MethodHandle creator;
        private final Method updateMethod;
        private final MethodHandle resultMethod;
        private final int[] indexesToMap;

        ReflectiveUserAggregationFunction( UserFunctionSignature signature, MethodHandle constructor,
                MethodHandle creator, Method updateMethod, MethodHandle resultMethod,
                TypeMappers.TypeChecker typeChecker,
                List<FieldSetter> fieldSetters )
        {
            super( fieldSetters );
            this.constructor = constructor;
            this.creator = creator;
            this.updateMethod = updateMethod;
            this.resultMethod = resultMethod;
            this.signature = signature;
            this.typeChecker = typeChecker;
            this.indexesToMap = computeIndexesToMap( signature.inputSignature() );
        }

        @Override
        public UserFunctionSignature signature()
        {
            return signature;
        }

        @Override
        public UserAggregator create( Context ctx ) throws ProcedureException
        {
            // For now, create a new instance of the class for each invocation. In the future, we'd like to keep
            // instances local to
            // at least the executing session, but we don't yet have good interfaces to the kernel to model that with.
            try
            {

                Object cls = constructor.invoke();
                //API injection
                inject( ctx, cls );
                Object aggregator = creator.invoke( cls );
                List<FieldSignature> inputSignature = signature.inputSignature();
                int expectedNumberOfInputs = inputSignature.size();

                return new UserAggregator()
                {
                    @Override
                    public void update( AnyValue[] input ) throws ProcedureException
                    {
                        try
                        {
                            if ( expectedNumberOfInputs != input.length )
                            {
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                                        "Function `%s` takes %d arguments but %d was provided.",
                                        signature.name(),
                                        expectedNumberOfInputs, input.length );
                            }

                            ValueMapper<Object> mapper = ctx.valueMapper();

                            // Call the method
                            updateMethod.invoke( aggregator, mapToObjects( "Function", signature.name(), mapper, signature.inputSignature(), input ) );
                        }
                        catch ( Throwable throwable )
                        {
                            if ( throwable instanceof Status.HasStatus )
                            {
                                throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                                        throwable.getMessage() );
                            }
                            else
                            {
                                Throwable cause = ExceptionUtils.getRootCause( throwable );
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + (cause != null ? cause : throwable) );
                            }
                        }
                    }

                    @Override
                    public AnyValue result() throws ProcedureException
                    {
                        try
                        {
                            return typeChecker.toValue( resultMethod.invoke(aggregator) );
                        }
                        catch ( Throwable throwable )
                        {
                            if ( throwable instanceof Status.HasStatus )
                            {
                                throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                                        throwable.getMessage() );
                            }
                            else
                            {
                                Throwable cause = ExceptionUtils.getRootCause( throwable );
                                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                                        "Failed to invoke function `%s`: %s", signature.name(),
                                        "Caused by: " + (cause != null ? cause : throwable) );
                            }
                        }

                    }

                };

            }
            catch ( Throwable throwable )
            {
                if ( throwable instanceof Status.HasStatus )
                {
                    throw new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                            throwable.getMessage() );
                }
                else
                {
                    Throwable cause = ExceptionUtils.getRootCause( throwable );
                    throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                            "Failed to invoke function `%s`: %s", signature.name(),
                            "Caused by: " + (cause != null ? cause : throwable ) );
                }
            }
        }
    }

    private static void rejectEmptyNamespace( QualifiedName name ) throws ProcedureException
    {
        if ( name.namespace() == null || name.namespace().length == 0 )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "It is not allowed to define functions in the root namespace please use a namespace, " +
                    "e.g. `@UserFunction(\"org.example.com.%s\")", name.name() );
        }
    }

    private static int[] computeIndexesToMap( List<FieldSignature> inputSignature )
    {
        ArrayList<Integer> integers = new ArrayList<>();
        for ( int i = 0; i < inputSignature.size(); i++ )
        {
            if ( inputSignature.get( i ).needsMapping() )
            {
                integers.add( i );
            }
        }
        return integers.stream().mapToInt( i -> i ).toArray();
    }
}
