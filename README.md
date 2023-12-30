# Distributed systems

This repository contains my solutions for programming labs from [6.5840: Distributed Systems (Spring 2023)](https://pdos.csail.mit.edu/6.824/)


> 6.5840 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. 
> It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, 
> replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

All of the details regarding the building/compilation/running/testing process and the tasks themselves can be found on [the page of the course](https://pdos.csail.mit.edu/6.824/).

**I do not study at MIT** and do these labs purely for my enjoyment and growth, so there may be some inconsistencies and mistakes that I didn't notice.

## Lab 1: MapReduce

The code is located in: `src/mr/*.go`

For easier debugging and testing, I additionally added an `mr.sh` script. It requires [GNU parallel](https://www.gnu.org/software/parallel/) to work and starts the map-reduce with a selected number of workers and selected plugin (picking `src/main/pg-*` as input).

## Lab 2: ...