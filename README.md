PAP
======

PAP ("Program analysis pipeline") is a binary analysis platform designed for
creating and running pipelines of binary analysis tools. It is inspired by
github actions, but makes design choices that are optimized for binary analysis
workflows rather than general software development workflows.

## Project structure

PAP consists of a number of crates designed to maximize code reusability.
Currently, these are:

- `pap-config` - A crate for defining the configuration of a PAP pipeline.
- `pap-pipeline` - A library crate for executing PAP pipelines.
- `pap-run` - A binary crate for running PAP pipelines locally.
