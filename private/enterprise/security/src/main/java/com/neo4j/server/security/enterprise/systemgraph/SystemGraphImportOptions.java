/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.RoleRepository;

import java.util.function.Supplier;

import org.neo4j.server.security.auth.UserRepository;

public class SystemGraphImportOptions
{
    boolean mayPerformMigration;
    Supplier<UserRepository> migrationUserRepositorySupplier;
    Supplier<RoleRepository> migrationRoleRepositorySupplier;
    Supplier<UserRepository> initialUserRepositorySupplier;
    Supplier<UserRepository> defaultAdminRepositorySupplier;

    public SystemGraphImportOptions(
            boolean mayPerformMigration,
            Supplier<UserRepository> migrationUserRepositorySupplier,
            Supplier<RoleRepository> migrationRoleRepositorySupplier,
            Supplier<UserRepository> initialUserRepositorySupplier,
            Supplier<UserRepository> defaultAdminRepositorySupplier
    )
    {
        this.mayPerformMigration = mayPerformMigration;
        this.migrationUserRepositorySupplier = migrationUserRepositorySupplier;
        this.migrationRoleRepositorySupplier = migrationRoleRepositorySupplier;
        this.initialUserRepositorySupplier = initialUserRepositorySupplier;
        this.defaultAdminRepositorySupplier = defaultAdminRepositorySupplier;
    }

}
