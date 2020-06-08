/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.neo4j.bench.model.options.Edition;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Project
{

    public static final String NAME = "name";
    public static final String COMMIT = "commit";
    public static final String VERSION = "version";
    public static final String EDITION = "edition";
    public static final String BRANCH = "branch";
    public static final String OWNER = "owner";

    protected final Repository repository;
    protected final String commit;
    protected final String version;
    protected final Edition edition;
    protected final String branch;
    protected final String owner;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Project()
    {
        this( Repository.NEO4J, "1", "1.2.3", Edition.COMMUNITY, "1", "1" );
    }

    /**
     * Class fields are used to construct github URIs in the following way:
     * <p>
     * https://github.com/{owner}/{name}/commit/{commit}
     * <p>
     * E.g., this URI:
     * <p>
     * https://github.com/neo4j/neo4j/commit/489a79f8a7b097b7fafde8c3378c47766089ce00
     * <p>
     * Maps to:
     * <ul>
     * <li>name = "neo4j"</li>
     * <li>commit = "489a79f8a7b097b7fafde8c3378c47766089ce00"</li>
     * <li>owner = "neo4j"</li>
     * </ul
     *
     * @param repository
     * @param commit
     * @param version
     * @param edition
     * @param branch
     * @param owner
     */
    public Project( Repository repository, String commit, String version, Edition edition, String branch, String owner )
    {
        repository.assertValidVersion( version );
        this.repository = requireNonNull( repository );
        this.commit = requireNonNull( commit );
        this.version = requireNonNull( version );
        this.edition = requireNonNull( edition );
        this.branch = requireNonNull( branch );
        this.owner = requireNonNull( owner );
    }

    public Repository repository()
    {
        return repository;
    }

    public String commit()
    {
        return commit;
    }

    public String version()
    {
        return version;
    }

    public Edition edition()
    {
        return edition;
    }

    public String branch()
    {
        return branch;
    }

    public String owner()
    {
        return owner;
    }

    public String name()
    {
        return repository.projectName();
    }

    public Map<String,String> toMap()
    {
        HashMap<String,String> map = new HashMap<>();
        map.put( NAME, repository.projectName().toLowerCase() );
        map.put( COMMIT, commit.toLowerCase() );
        map.put( EDITION, edition.name().toLowerCase() );
        map.put( VERSION, version );
        map.put( BRANCH, branch.toLowerCase() );
        map.put( OWNER, owner.toLowerCase() );
        return map;
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
        Project project = (Project) o;
        return repository == project.repository &&
               Objects.equals( commit, project.commit ) &&
               Objects.equals( version, project.version ) &&
               Objects.equals( edition, project.edition ) &&
               Objects.equals( branch, project.branch ) &&
               Objects.equals( owner, project.owner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( repository, commit, version, edition, branch, owner );
    }

    @Override
    public String toString()
    {
        return "Project{" +
               "name=" + name() +
               ", commit='" + commit + '\'' +
               ", version='" + version + '\'' +
               ", edition='" + edition + '\'' +
               ", branch='" + branch + '\'' +
               ", owner='" + owner + '\'' +
               '}';
    }
}
