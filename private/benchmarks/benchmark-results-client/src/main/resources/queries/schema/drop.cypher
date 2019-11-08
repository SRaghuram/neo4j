// Unique
DROP CONSTRAINT ON ( annotation:Annotation ) ASSERT annotation.event_id IS UNIQUE
DROP CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT benchmarktool.name IS UNIQUE
DROP CONSTRAINT ON ( plantree:PlanTree ) ASSERT plantree.description_hash IS UNIQUE
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT testrun.archive IS UNIQUE
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT testrun.id IS UNIQUE
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT testrun.jobId IS UNIQUE
// Exists
DROP CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.author)
DROP CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.comment)
DROP CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.date)
DROP CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.event_id)
DROP CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.description)
DROP CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.mode)
DROP CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.name)
DROP CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.simple_name)
DROP CONSTRAINT ON ( benchmarkgroup:BenchmarkGroup ) ASSERT exists(benchmarkgroup.name)
DROP CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT exists(benchmarktool.name)
DROP CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT exists(benchmarktool.repository_name)
DROP CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.branch)
DROP CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.commit)
DROP CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.owner)
DROP CONSTRAINT ON ( environment:Environment ) ASSERT exists(environment.operating_system)
DROP CONSTRAINT ON ( environment:Environment ) ASSERT exists(environment.server)
DROP CONSTRAINT ON ( java:Java ) ASSERT exists(java.args)
DROP CONSTRAINT ON ( java:Java ) ASSERT exists(java.jvm)
DROP CONSTRAINT ON ( java:Java ) ASSERT exists(java.version)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.error)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.max)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.mean)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.min)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_25)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_50)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_75)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_90)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_95)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_99)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_99_9)
DROP CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.sample_size)
DROP CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.db_hits)
DROP CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.operator_type)
DROP CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.rows)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.cypher_version)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.default_planner)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.default_runtime)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.requested_planner)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.requested_runtime)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.used_planner)
DROP CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.used_runtime)
DROP CONSTRAINT ON ( plantree:PlanTree ) ASSERT exists(plantree.description)
DROP CONSTRAINT ON ( plantree:PlanTree ) ASSERT exists(plantree.description_hash)
DROP CONSTRAINT ON ( project:Project ) ASSERT exists(project.branch)
DROP CONSTRAINT ON ( project:Project ) ASSERT exists(project.commit)
DROP CONSTRAINT ON ( project:Project ) ASSERT exists(project.name)
DROP CONSTRAINT ON ( project:Project ) ASSERT exists(project.owner)
DROP CONSTRAINT ON ( project:Project ) ASSERT exists(project.version)
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.date)
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.duration)
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.id)
DROP CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.triggered_by)
// Index
DROP INDEX ON :Annotation(author)
DROP INDEX ON :Annotation(comment)
DROP INDEX ON :Annotation(date)
DROP INDEX ON :Benchmark(mode)
DROP INDEX ON :Benchmark(name)
DROP INDEX ON :Benchmark(simple_name)
DROP INDEX ON :BenchmarkGroup(name)
DROP INDEX ON :BenchmarkTool(repository_name)
DROP INDEX ON :BenchmarkToolVersion(branch)
DROP INDEX ON :BenchmarkToolVersion(commit)
DROP INDEX ON :BenchmarkToolVersion(owner)
DROP INDEX ON :Environment(operating_system)
DROP INDEX ON :Environment(server)
DROP INDEX ON :Java(jvm)
DROP INDEX ON :Java(version)
DROP INDEX ON :Operator(operator_type)
DROP INDEX ON :Plan(cypher_version)
DROP INDEX ON :Plan(default_planner)
DROP INDEX ON :Plan(default_runtime)
DROP INDEX ON :Plan(requested_planner)
DROP INDEX ON :Plan(requested_runtime)
DROP INDEX ON :Plan(used_planner)
DROP INDEX ON :Plan(used_runtime)
DROP INDEX ON :Project(branch)
DROP INDEX ON :Project(commit)
DROP INDEX ON :Project(edition)
DROP INDEX ON :Project(name)
DROP INDEX ON :Project(owner)
DROP INDEX ON :Project(version)
DROP INDEX ON :TestRun(build)
DROP INDEX ON :TestRun(date)
DROP INDEX ON :TestRun(duration)
DROP INDEX ON :TestRun(parent_build)
