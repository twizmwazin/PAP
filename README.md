PAP
======

PAP ("Program analysis pipeline") is a binary analysis platform designed for
creating and running pipelines of binary analysis tools. It is inspired by
github actions, but makes design choices that are optimized for binary analysis
workflows rather than general software development workflows.

## Project structure

PAP consists of a number of crates designed to maximize code reusability.
Currently, these are:

- `pap-api` - A crate that defines all of the public API types used by PAP.
- `pap-client` - A CLI client for interacting with PAP servers.
- `pap-run` - A program to run one-off PAP pipelines.
- `pap-server` - A server that runs PAP pipelines submitted over the network.
