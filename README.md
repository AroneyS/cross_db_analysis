# Find elusive but prevalent OTUs (i.e. common but unbinned organisms)

## Inputs: SingleM Databases

SingleM databases created with combined reads, bins and, optionally, assemblies.

## Outputs: Database or CSV file

Database containing table with row for each OTU present in reads. Each row has
taxonomy, marker_id, sum(coverage) and flags indicating if the OTU is present
in the bins or assemblies and if not, the taxonomy of the nearest bin/assembly OTU.

## Steps

1. Load output database as SQLITE3 database (as temporary database if unspecified).
1. ATTACH reads, assemblies and bins databases.
1. CREATE summary tables for each database, deduplicating taxonomy and summing coverage.
1. CREATE compare table listing reads OTUs and whether they are present in bins/assemblies.
1. Output list of elusive but prevalent OTUs.
   - Genus/Species resolution
   - High max(sum(coverage)) across marker genes
   - No matching bins
