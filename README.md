# Find elusive but prevalent OTUs (i.e. common but unbinned organisms)

## Inputs: SingleM Databases

SingleM databases created with combined reads, bins and, optionally, assemblies.

## Outputs: Database or CSV file

Database containing table with row for each OTU present in reads. Each row has
a flag indicating if the OTU is present in the bins or assemblies and if not,
the taxonomy of the nearest bin/assembly OTU.

## Steps

1. Load output database as SQLITE3 database. (as temporary database if unspecified)
1. ATTACH reads, assemblies and bins databases.
1. Form cross database table for comparisons between databases, deduplicating taxonomy.
1. Search for nearest bin/assembly OTU for non-matching read OTUs.
1. Output list of elusive but prevalent OTUs.
