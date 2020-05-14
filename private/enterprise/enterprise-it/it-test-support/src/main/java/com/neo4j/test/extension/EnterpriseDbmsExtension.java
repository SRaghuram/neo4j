/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

/**
 * Provides test access to {@link DatabaseManagementService} running enterprise edition.
 * If you want an impermanent version, see {@link ImpermanentEnterpriseDbmsExtension}.
 *
 * <p>This extension will inject the following fields, if available.
 * <ul>
 *     <li>{@link FileSystemAbstraction} as {@link DefaultFileSystemExtension}.</li>
 *     <li>{@link TestDirectory}.</li>
 *     <li>{@link DatabaseManagementService}.</li>
 *     <li>{@link GraphDatabaseService}.</li>
 *     <li>{@link GraphDatabaseAPI}.</li>
 * </ul>
 *
 * <p>The life time of the {@link DatabaseManagementService} will be for the duration of
 * the whole test class. Each test method will receive a newly created database with a
 * unique name. After the test method the database will be shutdown, not dropped, to allow
 * inspection upon failures.
 *
 * <p>You can specify a callback with {@link #configurationCallback()}, this callback is invoked just before
 * the {@link DatabaseManagementServiceBuilder} completes, allowing further modifications, e.g. adding additional
 * dependencies, injecting a monitor or extension etc.
 */
@Inherited
@Target( ElementType.TYPE )
@Retention( RetentionPolicy.RUNTIME )
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@Neo4jLayoutExtension
@ExtendWith( EnterpriseDbmsSupportExtension.class )
public @interface EnterpriseDbmsExtension
{
    /**
     * Name of a void method that takes a {@link TestDatabaseManagementServiceBuilder} as parameter. The method
     * must be annotated with {@link ExtensionCallback}. This can be used to issue additional commands on the
     * builder before it completes. This method will be invoked <strong>BEFORE</strong> any method annotated with
     * {@link BeforeAll}, this is because the injected fields should be available in the BeforeAll context.
     *
     * <p>One example is to set some additional configuration values:
     * <pre>{@code
     *     @ExtensionCallback
     *     void configuration( TestDatabaseManagementServiceBuilder builder )
     *     {
     *         builder.setConfig( ... );
     *     }
     * }</pre>
     */
    String configurationCallback() default "";
}
