// Unique
CREATE CONSTRAINT ON ( annotation:Annotation ) ASSERT annotation.event_id IS UNIQUE
CREATE CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT benchmarktool.name IS UNIQUE
CREATE CONSTRAINT ON ( plantree:PlanTree ) ASSERT plantree.description_hash IS UNIQUE
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT testrun.archive IS UNIQUE
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT testrun.id IS UNIQUE
// Exists
CREATE CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.author)
CREATE CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.comment)
CREATE CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.date)
CREATE CONSTRAINT ON ( annotation:Annotation ) ASSERT exists(annotation.event_id)
CREATE CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.description)
CREATE CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.mode)
CREATE CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.name)
CREATE CONSTRAINT ON ( benchmark:Benchmark ) ASSERT exists(benchmark.simple_name)
CREATE CONSTRAINT ON ( benchmarkgroup:BenchmarkGroup ) ASSERT exists(benchmarkgroup.name)
CREATE CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT exists(benchmarktool.name)
CREATE CONSTRAINT ON ( benchmarktool:BenchmarkTool ) ASSERT exists(benchmarktool.repository_name)
CREATE CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.branch)
CREATE CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.commit)
CREATE CONSTRAINT ON ( benchmarktoolversion:BenchmarkToolVersion ) ASSERT exists(benchmarktoolversion.owner)
CREATE CONSTRAINT ON ( environment:Environment ) ASSERT exists(environment.operating_system)
CREATE CONSTRAINT ON ( environment:Environment ) ASSERT exists(environment.server)
CREATE CONSTRAINT ON ( java:Java ) ASSERT exists(java.args)
CREATE CONSTRAINT ON ( java:Java ) ASSERT exists(java.jvm)
CREATE CONSTRAINT ON ( java:Java ) ASSERT exists(java.version)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.error)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.max)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.mean)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.min)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_25)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_50)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_75)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_90)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_95)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_99)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.perc_99_9)
CREATE CONSTRAINT ON ( metrics:Metrics ) ASSERT exists(metrics.sample_size)
CREATE CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.db_hits)
CREATE CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.operator_type)
CREATE CONSTRAINT ON ( operator:Operator ) ASSERT exists(operator.rows)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.cypher_version)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.default_planner)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.default_runtime)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.requested_planner)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.requested_runtime)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.used_planner)
CREATE CONSTRAINT ON ( plan:Plan ) ASSERT exists(plan.used_runtime)
CREATE CONSTRAINT ON ( plantree:PlanTree ) ASSERT exists(plantree.description)
CREATE CONSTRAINT ON ( plantree:PlanTree ) ASSERT exists(plantree.description_hash)
CREATE CONSTRAINT ON ( project:Project ) ASSERT exists(project.branch)
CREATE CONSTRAINT ON ( project:Project ) ASSERT exists(project.commit)
CREATE CONSTRAINT ON ( project:Project ) ASSERT exists(project.name)
CREATE CONSTRAINT ON ( project:Project ) ASSERT exists(project.owner)
CREATE CONSTRAINT ON ( project:Project ) ASSERT exists(project.version)
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.date)
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.duration)
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.id)
CREATE CONSTRAINT ON ( testrun:TestRun ) ASSERT exists(testrun.triggered_by)
// Index
CREATE INDEX ON :Annotation(author)
CREATE INDEX ON :Annotation(comment)
CREATE INDEX ON :Annotation(date)
CREATE INDEX ON :Benchmark(mode)
CREATE INDEX ON :Benchmark(name)
CREATE INDEX ON :Benchmark(simple_name)
CREATE INDEX ON :BenchmarkGroup(name)
CREATE INDEX ON :BenchmarkTool(repository_name)
CREATE INDEX ON :BenchmarkToolVersion(branch)
CREATE INDEX ON :BenchmarkToolVersion(commit)
CREATE INDEX ON :BenchmarkToolVersion(owner)
CREATE INDEX ON :Environment(operating_system)
CREATE INDEX ON :Environment(server)
CREATE INDEX ON :Java(jvm)
CREATE INDEX ON :Java(version)
CREATE INDEX ON :Operator(operator_type)
CREATE INDEX ON :Plan(cypher_version)
CREATE INDEX ON :Plan(default_planner)
CREATE INDEX ON :Plan(default_runtime)
CREATE INDEX ON :Plan(requested_planner)
CREATE INDEX ON :Plan(requested_runtime)
CREATE INDEX ON :Plan(used_planner)
CREATE INDEX ON :Plan(used_runtime)
CREATE INDEX ON :Project(branch)
CREATE INDEX ON :Project(commit)
CREATE INDEX ON :Project(edition)
CREATE INDEX ON :Project(name)
CREATE INDEX ON :Project(owner)
CREATE INDEX ON :Project(version)
CREATE INDEX ON :TestRun(build)
CREATE INDEX ON :TestRun(date)
CREATE INDEX ON :TestRun(duration)
CREATE INDEX ON :TestRun(parent_build)
