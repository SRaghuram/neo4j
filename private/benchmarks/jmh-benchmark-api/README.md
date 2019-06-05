== Backlog

- Turn this README into instructions
- Improve / Rewrite Config File
  - use a nested format (e.g. JSON) to improve readability / simplify parsing and generation
  - allow configuration of per benchmark thread counts
  - warmup settings per benchmark
  - SuiteDescription.fromConfig should not take a SuiteDescription as input parameter
- Overhaul Validation
  - instead of storing strings, we should store references to the classes that contained errors, as well as the reasons
