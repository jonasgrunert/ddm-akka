## Thinking

We would like to parallelize as much and start working on things as soon as we know the task and have a worker up.

While not the fastest method a good idea could be too dedicate all workers to first calculate all hashes possible in the universe so to build a map of all strings and hashes.

Following that we may want to hand each worker one row of the file or something like that. Then an actor can figure out all the hints by requesting the master to send over the string for a specific hash.

## Work done
[ ] File parsing
[ ] Adding messages
[ ] Adding hash calculation