############################
### cross_db_analysis.py ###
############################
# Author: Samuel Aroney
# Find prevalent OTUs that are not assembled/binned
# Compare read/assembly/bin SingleM databases

# Manual test code
# python cross_db_analysis.py \
# --reads-db /home/aroneys/projects/01-singleM-coassembly/results/singlem_reads_combined.sdb \
# --bins-db /home/aroneys/projects/01-singleM-coassembly/results/singlem_bins_combined.sdb \
# --output-db /home/aroneys/src/cross_db_analysis/output/output.db \
# --output /home/aroneys/src/cross_db_analysis/output/output.csv

import sqlite3
import logging
import argparse


class SqliteDatabase:
    def __init__(self, db_path):
        self.connection = sqlite3.Connection(db_path)
        self.cursor = sqlite3.Cursor(self.connection)
    
    def execute(self, sql):
        return self.cursor.execute(sql)

class CrossDatabaseComparator:
    def __init__(self, **kwargs):
        logging.info("Initialising CrossDatabaseComparator")
        self.reads_db_path = kwargs.pop('reads_db')
        self.assemblies_db_path = kwargs.pop('assemblies_db')
        self.bins_db_path = kwargs.pop('bins_db')
        self.output_db_path = kwargs.pop('output_db')
        self.output_path = kwargs.pop('output')

        if self.output_db_path:
            logging.info(f"Creating output database at: {self.output_db_path}")
            self.output_db = self._connect_database(self.output_db_path)
        else:
            logging.info("Creating temporary output database")
            self.output_db = self._connect_database('')
    
    def _connect_database(self, db_path):
        db = SqliteDatabase(db_path)
        return db


def main():
    parser = argparse.ArgumentParser(description='Find prevalent OTUs that are not assembled/binned.')
    parser.add_argument('--debug', help='output debug information', action="store_true")
    parser.add_argument('--quiet', action="store_true")
    parser.add_argument('--reads-db', type=str, required=True,
                        metavar='<READS DB>', help='path to reads SingleM database')
    parser.add_argument('--assemblies-db', type=str,
                        metavar='<ASSEMBLIES DB>', help='path to assemblies SingleM database')
    parser.add_argument('--bins-db', type=str, required=True,
                        metavar='<BINS DB>', help='path to bins SingleM database')
    parser.add_argument('--output-db', type=str,
                        metavar='<OUTPUT DB>', help='path to output SQLite3 database')
    parser.add_argument('--output', type=str, required=True,
                        metavar='<OUTPUT>', help='path to output csv file')

    args = parser.parse_args()
    if args.debug:
        loglevel = logging.DEBUG
    elif args.quiet:
        loglevel = logging.ERROR
    else:
        loglevel = logging.INFO
    logging.basicConfig(level=loglevel, format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    

    reads_db_path = getattr(args, 'reads_db')
    assemblies_db_path = getattr(args, 'assemblies_db', False)
    bins_db_path = getattr(args, 'bins_db')
    output_db_path = getattr(args, 'output_db', False)
    output_path = getattr(args, 'output')

    # print("reads: %s, assem: %s, bins: %s, output: %s, outputcsv: %s" %
    #       (reads_db_path, assemblies_db_path, bins_db_path, output_db_path, output_path))

    CrossDatabaseComparator(
            reads_db=reads_db_path,
            assemblies_db=assemblies_db_path,
            bins_db=bins_db_path,
            output_db=output_db_path,
            output=output_path)


if __name__ == "__main__":
    main()
