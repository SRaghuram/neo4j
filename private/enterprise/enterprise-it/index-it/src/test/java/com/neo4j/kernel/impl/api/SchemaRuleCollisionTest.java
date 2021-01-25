/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

@EphemeralTestDirectoryExtension
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class SchemaRuleCollisionTest
{
    @Inject
    TestDirectory testDirectory;

    /* DBMS and DB stay alive through whole test class and are simply cleaned between tests */
    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;

    @BeforeAll
    void setupDb()
    {
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setFileSystem( testDirectory.getFileSystem() )
                .impermanent()
                .build();
        db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
    }

    @AfterAll
    void tearDownDb()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
        }
    }

    @BeforeEach
    void cleanSchema()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().getConstraints().forEach( ConstraintDefinition::drop );
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    private static final SchemaType<IndexDescriptor> INDEX = createLabelSchemaType( SchemaWrite::indexCreate, "index" );
    private static final SchemaType<ConstraintDescriptor> UNIQUE_CONSTRAINT = createLabelSchemaType(
            ( schemaWrite, schema, name ) -> schemaWrite.uniquePropertyConstraintCreate( uniqueForSchema( schema ).withName( name ) ), "unique_constraint" );
    private static final SchemaType<ConstraintDescriptor> NODE_KEY_CONSTRAINT = createLabelSchemaType(
            ( schemaWrite, schema, name ) -> schemaWrite.nodeKeyConstraintCreate( uniqueForSchema( schema ).withName( name ) ), "node_key_constraint" );
    private static final SchemaType<ConstraintDescriptor> EXISTENCE_CONSTRAINT =
            createLabelSchemaType( SchemaWrite::nodePropertyExistenceConstraintCreate, "existence_constraint" );

    private static List<Arguments> allSchemaTypes()
    {
        return List.of(
                arguments( INDEX ),
                arguments( UNIQUE_CONSTRAINT ),
                arguments( NODE_KEY_CONSTRAINT ),
                arguments( EXISTENCE_CONSTRAINT )
        );
    }

    private static List<Arguments> paramsForDifferentNameDifferentSchema()
    {
        return List.of(
                //       ┌────────────────────────────────────────────────────────────────────┐
                //       │                  DIFFERENT NAME DIFFERENT SCHEMA                   │
                //       ├─────────────────────┬─────────────────────┬────────────────────────┤
                //       │ EXISTING SCHEMA     │ CREATE SCHEMA       │ ExpectedResult         │
                //       └─────────────────────┴─────────────────────┴────────────────────────┘
                arguments( INDEX,                INDEX,                Result.success() ),
                arguments( INDEX,                UNIQUE_CONSTRAINT,    Result.success() ),
                arguments( INDEX,                NODE_KEY_CONSTRAINT,  Result.success() ),
                arguments( INDEX,                EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( UNIQUE_CONSTRAINT,    INDEX,                Result.success() ),
                arguments( UNIQUE_CONSTRAINT,    UNIQUE_CONSTRAINT,    Result.success() ),
                arguments( UNIQUE_CONSTRAINT,    NODE_KEY_CONSTRAINT,  Result.success() ),
                arguments( UNIQUE_CONSTRAINT,    EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( NODE_KEY_CONSTRAINT,  INDEX,                Result.success() ),
                arguments( NODE_KEY_CONSTRAINT,  UNIQUE_CONSTRAINT,    Result.success() ),
                arguments( NODE_KEY_CONSTRAINT,  NODE_KEY_CONSTRAINT,  Result.success() ),
                arguments( NODE_KEY_CONSTRAINT,  EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( EXISTENCE_CONSTRAINT, INDEX,                Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, UNIQUE_CONSTRAINT,    Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, NODE_KEY_CONSTRAINT,  Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, EXISTENCE_CONSTRAINT, Result.success() )
        );
    }

    private static List<Arguments> paramsForDifferentNameSameSchema()
    {
        return List.of(
                //       ┌────────────────────────────────────────────────────────────────────┐
                //       │                   DIFFERENT NAME SAME SCHEMA                       │
                //       ├─────────────────────┬─────────────────────┬────────────────────────┤
                //       │ EXISTING SCHEMA     │ CREATE SCHEMA       │ ExpectedResult         │
                //       └─────────────────────┴─────────────────────┴────────────────────────┘
                arguments( INDEX,                INDEX,                alreadyIndexed() ),
                arguments( INDEX,                UNIQUE_CONSTRAINT,    alreadyIndexed() ),
                arguments( INDEX,                NODE_KEY_CONSTRAINT,  alreadyIndexed() ),
                arguments( INDEX,                EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( UNIQUE_CONSTRAINT,    INDEX,                alreadyConstrained() ),
                arguments( UNIQUE_CONSTRAINT,    UNIQUE_CONSTRAINT,    alreadyConstrained() ),
                arguments( UNIQUE_CONSTRAINT,    NODE_KEY_CONSTRAINT,  alreadyConstrained() ),
                arguments( UNIQUE_CONSTRAINT,    EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( NODE_KEY_CONSTRAINT,  INDEX,                alreadyConstrained() ),
                arguments( NODE_KEY_CONSTRAINT,  UNIQUE_CONSTRAINT,    alreadyConstrained() ),
                arguments( NODE_KEY_CONSTRAINT,  NODE_KEY_CONSTRAINT,  alreadyConstrained() ),
                arguments( NODE_KEY_CONSTRAINT,  EXISTENCE_CONSTRAINT, Result.success() ),

                arguments( EXISTENCE_CONSTRAINT, INDEX,                Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, UNIQUE_CONSTRAINT,    Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, NODE_KEY_CONSTRAINT,  Result.success() ),
                arguments( EXISTENCE_CONSTRAINT, EXISTENCE_CONSTRAINT, alreadyConstrained() )
        );
    }

    private static List<Arguments> paramsForSameNameDifferentSchema()
    {
        return List.of(
                //       ┌────────────────────────────────────────────────────────────────────┐
                //       │                   SAME NAME DIFFERENT SCHEMA                       │
                //       ├─────────────────────┬─────────────────────┬────────────────────────┤
                //       │ EXISTING SCHEMA     │ CREATE SCHEMA       │ ExpectedResult         │
                //       └─────────────────────┴─────────────────────┴────────────────────────┘
                arguments( INDEX,                INDEX,                indexWithNameAlreadyExists() ),
                arguments( INDEX,                UNIQUE_CONSTRAINT,    indexWithNameAlreadyExists() ),
                arguments( INDEX,                NODE_KEY_CONSTRAINT,  indexWithNameAlreadyExists() ),
                arguments( INDEX,                EXISTENCE_CONSTRAINT, indexWithNameAlreadyExists() ),

                arguments( UNIQUE_CONSTRAINT,    INDEX,                constraintWithNameAlreadyExists() ),
                arguments( UNIQUE_CONSTRAINT,    UNIQUE_CONSTRAINT,    constraintWithNameAlreadyExists() ),
                arguments( UNIQUE_CONSTRAINT,    NODE_KEY_CONSTRAINT,  constraintWithNameAlreadyExists() ),
                arguments( UNIQUE_CONSTRAINT,    EXISTENCE_CONSTRAINT, constraintWithNameAlreadyExists() ),

                arguments( NODE_KEY_CONSTRAINT,  INDEX,                constraintWithNameAlreadyExists() ),
                arguments( NODE_KEY_CONSTRAINT,  UNIQUE_CONSTRAINT,    constraintWithNameAlreadyExists() ),
                arguments( NODE_KEY_CONSTRAINT,  NODE_KEY_CONSTRAINT,  constraintWithNameAlreadyExists() ),
                arguments( NODE_KEY_CONSTRAINT,  EXISTENCE_CONSTRAINT, constraintWithNameAlreadyExists() ),

                arguments( EXISTENCE_CONSTRAINT, INDEX,                constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, UNIQUE_CONSTRAINT,    constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, NODE_KEY_CONSTRAINT,  constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, EXISTENCE_CONSTRAINT, constraintWithNameAlreadyExists() )
        );
    }

    private static List<Arguments> paramsForSameNameSameSchema()
    {
        return List.of(
                //       ┌────────────────────────────────────────────────────────────────────┐
                //       │                      SAME NAME SAME SCHEMA                         │
                //       ├─────────────────────┬─────────────────────┬────────────────────────┤
                //       │ EXISTING SCHEMA     │ CREATE SCHEMA       │ ExpectedResult         │
                //       └─────────────────────┴─────────────────────┴────────────────────────┘
                arguments( INDEX,                INDEX,                equivalentSchemaRuleExists() ),
                arguments( INDEX,                UNIQUE_CONSTRAINT,    indexWithNameAlreadyExists() ),
                arguments( INDEX,                NODE_KEY_CONSTRAINT,  indexWithNameAlreadyExists() ),
                arguments( INDEX,                EXISTENCE_CONSTRAINT, indexWithNameAlreadyExists() ),

                arguments( UNIQUE_CONSTRAINT,    INDEX,                constraintWithNameAlreadyExists() ),
                arguments( UNIQUE_CONSTRAINT,    UNIQUE_CONSTRAINT,    equivalentSchemaRuleExists() ),
                arguments( UNIQUE_CONSTRAINT,    NODE_KEY_CONSTRAINT,  constraintWithNameAlreadyExists() ),
                arguments( UNIQUE_CONSTRAINT,    EXISTENCE_CONSTRAINT, constraintWithNameAlreadyExists() ),

                arguments( NODE_KEY_CONSTRAINT,  INDEX,                constraintWithNameAlreadyExists() ),
                arguments( NODE_KEY_CONSTRAINT,  UNIQUE_CONSTRAINT,    constraintWithNameAlreadyExists() ),
                arguments( NODE_KEY_CONSTRAINT,  NODE_KEY_CONSTRAINT,  equivalentSchemaRuleExists() ),
                arguments( NODE_KEY_CONSTRAINT,  EXISTENCE_CONSTRAINT, constraintWithNameAlreadyExists() ),

                arguments( EXISTENCE_CONSTRAINT, INDEX,                constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, UNIQUE_CONSTRAINT,    constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, NODE_KEY_CONSTRAINT,  constraintWithNameAlreadyExists() ),
                arguments( EXISTENCE_CONSTRAINT, EXISTENCE_CONSTRAINT, equivalentSchemaRuleExists() )
        );
    }

    @ParameterizedTest
    @MethodSource( "paramsForDifferentNameDifferentSchema" )
    void testSchemaCreationDifferentNameDifferentSchema( SchemaType schemaType1, SchemaType schemaType2, Result result ) throws KernelException
    {
        Pattern firstPattern = new Pattern( "label", "prop1" );
        Pattern secondPattern = new Pattern( "label", "prop2" );
        String firstName = "name";
        String secondName = "otherName";
        Result.success().verify( this, schemaType1, firstName, firstPattern );
        result.verify( this, schemaType2, secondName, secondPattern );
    }

    @ParameterizedTest
    @MethodSource( "paramsForDifferentNameSameSchema" )
    void testSchemaCreationDifferentNameSameSchema( SchemaType schemaType1, SchemaType schemaType2, Result result ) throws KernelException
    {
        Pattern pattern = new Pattern( "label", "prop" );
        String firstName = "name";
        String secondName = "otherName";
        Result.success().verify( this, schemaType1, firstName, pattern );
        result.verify( this, schemaType2, secondName, pattern );
    }

    @ParameterizedTest
    @MethodSource( "paramsForSameNameDifferentSchema" )
    void testSchemaCreationSameNameDifferentSchema( SchemaType schemaType1, SchemaType schemaType2, Result result ) throws KernelException
    {
        Pattern firstPattern = new Pattern( "label", "prop1" );
        Pattern secondPattern = new Pattern( "label", "prop2" );
        String name = "name";
        Result.success().verify( this, schemaType1, name, firstPattern );
        result.verify( this, schemaType2, name, secondPattern );
    }

    @ParameterizedTest
    @MethodSource( "paramsForSameNameSameSchema" )
    void testSchemaCreationSameNameSameSchema( SchemaType schemaType1, SchemaType schemaType2, Result result ) throws KernelException
    {
        Pattern pattern = new Pattern( "label", "prop" );
        String name = "name";
        Result.success().verify( this, schemaType1, name, pattern );
        result.verify( this, schemaType2, name, pattern );
    }

    @ParameterizedTest
    @MethodSource( "allSchemaTypes" )
    void testSchemaCreationWithEmptyName( SchemaType schemaType )
    {
        Pattern pattern = new Pattern( "label", "prop" );
        final IllegalArgumentException illegalArgumentException =
                assertThrows( IllegalArgumentException.class, () -> schemaType.create( db, "", pattern ) );
        assertEquals( "Schema rule name cannot be the empty string or only contain whitespace.", illegalArgumentException.getMessage() );
    }

    @Test
    void testRelationshipPropertyExistenceConstraintDifferentNameDifferentSchema() throws KernelException
    {
        final SchemaType<SchemaRule> relPropExistenceConstraint = createRelTypeSchemaType(
                SchemaWrite::relationshipPropertyExistenceConstraintCreate, "relationship_property_existence_constraint" );
        Pattern firstPattern = new Pattern( "relType", "prop1" );
        Pattern secondPattern = new Pattern( "relType", "prop2" );
        String firstName = "name";
        String secondName = "otherName";
        Result.success().verify( this, relPropExistenceConstraint, firstName, firstPattern );
        Result.success().verify( this, relPropExistenceConstraint, secondName, secondPattern );
    }

    @Test
    void testRelationshipPropertyExistenceConstraintDifferentNameSameSchema() throws KernelException
    {
        final SchemaType<SchemaRule> relPropExistenceConstraint = createRelTypeSchemaType(
                SchemaWrite::relationshipPropertyExistenceConstraintCreate, "relationship_property_existence_constraint" );
        Pattern pattern = new Pattern( "relType", "prop" );
        String firstName = "name";
        String secondName = "otherName";
        Result.success().verify( this, relPropExistenceConstraint, firstName, pattern );
        alreadyConstrained().verify( this, relPropExistenceConstraint, secondName, pattern );
    }

    @Test
    void testRelationshipPropertyExistenceConstraintSameNameDifferentSchema() throws KernelException
    {
        final SchemaType<SchemaRule> relPropExistenceConstraint = createRelTypeSchemaType(
                SchemaWrite::relationshipPropertyExistenceConstraintCreate, "relationship_property_existence_constraint" );
        Pattern firstPattern = new Pattern( "relType", "prop1" );
        Pattern secondPattern = new Pattern( "relType", "prop2" );
        String name = "name";
        Result.success().verify( this, relPropExistenceConstraint, name, firstPattern );
        constraintWithNameAlreadyExists().verify( this, relPropExistenceConstraint, name, secondPattern );
    }

    @Test
    void testRelationshipPropertyExistenceConstraintSameNameSameSchema() throws KernelException
    {
        final SchemaType<SchemaRule> relPropExistenceConstraint = createRelTypeSchemaType(
                SchemaWrite::relationshipPropertyExistenceConstraintCreate, "relationship_property_existence_constraint" );
        Pattern pattern = new Pattern( "relType", "prop" );
        String name = "name";
        Result.success().verify( this, relPropExistenceConstraint, name, pattern );
        equivalentSchemaRuleExists().verify( this, relPropExistenceConstraint, name, pattern );
    }

    private void assertExist( SchemaRule schemaRule ) throws TransactionFailureException
    {
        try ( KernelTransaction transaction = newTransaction( db ) )
        {
            final SchemaRead schemaRead = transaction.schemaRead();
            if ( schemaRule instanceof IndexDescriptor )
            {
                final IndexDescriptor indexDescriptor = (IndexDescriptor) schemaRule;
                final IndexDescriptor foundDescriptor = schemaRead.indexGetForName( indexDescriptor.getName() );
                assertEquals( indexDescriptor, foundDescriptor );
            }
            else if ( schemaRule instanceof ConstraintDescriptor )
            {
                final ConstraintDescriptor constraintDescriptor = (ConstraintDescriptor) schemaRule;
                final ConstraintDescriptor foundConstraint = schemaRead.constraintGetForName( constraintDescriptor.getName() );
                assertEquals( constraintDescriptor, foundConstraint );
            }
            transaction.commit();
        }
    }

    private void awaitIndexes()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
    }

    private static <T extends SchemaRule> SchemaType<T> createLabelSchemaType( SchemaWriteOperation<LabelSchemaDescriptor,T> operation, String opName )
    {
        return new SchemaType<>()
        {
            @Override
            public T create( GraphDatabaseAPI db, String name, Pattern pattern ) throws KernelException
            {
                final int labelId;
                final int propId;
                try ( KernelTransaction transaction = newTransaction( db ) )
                {
                    final TokenWrite tokenWrite = transaction.tokenWrite();
                    labelId = tokenWrite.labelGetOrCreateForName( pattern.labelName );
                    propId = tokenWrite.propertyKeyGetOrCreateForName( pattern.propertyName );
                    transaction.commit();
                }

                final LabelSchemaDescriptor schema = SchemaDescriptor.forLabel( labelId, propId );
                try ( KernelTransaction transaction = newTransaction( db ) )
                {
                    final SchemaWrite schemaWrite = transaction.schemaWrite();
                    final T result = operation.create( schemaWrite, schema, name );
                    transaction.commit();
                    return result;
                }
            }

            @Override
            public String toString()
            {
                return opName;
            }
        };
    }

    private static <T extends SchemaRule> SchemaType<T> createRelTypeSchemaType(
            SchemaWriteOperation<RelationTypeSchemaDescriptor,T> operation, String opName )
    {
        return new SchemaType<>()
        {
            @Override
            public T create( GraphDatabaseAPI db, String name, Pattern pattern ) throws KernelException
            {
                final int relTypeId;
                final int propId;
                try ( KernelTransaction transaction = newTransaction( db ) )
                {
                    final TokenWrite tokenWrite = transaction.tokenWrite();
                    relTypeId = tokenWrite.relationshipTypeGetOrCreateForName( pattern.labelName );
                    propId = tokenWrite.propertyKeyGetOrCreateForName( pattern.propertyName );
                    transaction.commit();
                }

                final RelationTypeSchemaDescriptor schema = SchemaDescriptor.forRelType( relTypeId, propId );
                try ( KernelTransaction transaction = newTransaction( db ) )
                {
                    final SchemaWrite schemaWrite = transaction.schemaWrite();
                    final T result = operation.create( schemaWrite, schema, name );
                    transaction.commit();
                    return result;
                }
            }

            @Override
            public String toString()
            {
                return opName;
            }
        };
    }

    private static KernelTransaction newTransaction( GraphDatabaseAPI db ) throws org.neo4j.internal.kernel.api.exceptions.TransactionFailureException
    {
        final Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        return kernel.beginTransaction( IMPLICIT, LoginContext.AUTH_DISABLED );
    }

    private static Result equivalentSchemaRuleExists()
    {
        return Result.fail( EquivalentSchemaRuleAlreadyExistsException.class );
    }

    private static Result indexWithNameAlreadyExists()
    {
        return Result.fail( IndexWithNameAlreadyExistsException.class );
    }

    private static Result constraintWithNameAlreadyExists()
    {
        return Result.fail( ConstraintWithNameAlreadyExistsException.class );
    }

    private static Result alreadyIndexed()
    {
        return Result.fail( AlreadyIndexedException.class );
    }

    private static Result alreadyConstrained()
    {
        return Result.fail( AlreadyConstrainedException.class );
    }

    interface SchemaWriteOperation<DESCRIPTOR extends SchemaDescriptor, T extends SchemaRule>
    {
        T create( SchemaWrite schemaWrite, DESCRIPTOR schemaDescriptor, String name ) throws KernelException;
    }

    private interface SchemaType<T extends SchemaRule>
    {
        T create( GraphDatabaseAPI db, String name, Pattern pattern ) throws KernelException;
    }

    private interface Result
    {
        static Result success()
        {
            return new Result()
            {
                @Override
                public void verify( SchemaRuleCollisionTest source, SchemaType schemaType, String name, Pattern pattern ) throws KernelException
                {
                    final SchemaRule schemaRule = schemaType.create( source.db, name, pattern );
                    source.awaitIndexes();
                    source.assertExist( schemaRule );
                }

                @Override
                public String toString()
                {
                    return "Expect[success]";
                }
            };
        }

        static <T extends Exception> Result fail( Class<T> clazz )
        {
            return new Result()
            {
                @Override
                public void verify( SchemaRuleCollisionTest source, SchemaType schemaType, String name, Pattern pattern )
                {
                    assertThrows( clazz, () -> schemaType.create( source.db, name, pattern ) );
                }

                @Override
                public String toString()
                {
                    return String.format( "Expect[%s]", clazz.getSimpleName() );
                }
            };
        }

        void verify( SchemaRuleCollisionTest source, SchemaType schemaType, String name, Pattern pattern ) throws KernelException;
    }

    private static class Pattern
    {
        private final String labelName;
        private final String propertyName;

        private Pattern( String labelName, String propertyName )
        {
            this.labelName = labelName;
            this.propertyName = propertyName;
        }

        @Override
        public String toString()
        {
            return String.format( ":%s(%s)", labelName, propertyName );
        }
    }
}
