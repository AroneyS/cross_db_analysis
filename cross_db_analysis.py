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
# --output /home/aroneys/src/cross_db_analysis/output/output.csv \
# --force

import sqlite3
import logging
import argparse
import os


class SqliteDatabase:
    def __init__(self, db_path):
        self.connection = sqlite3.Connection(db_path)
        self.cursor = sqlite3.Cursor(self.connection)
    
    def __del__(self):
        self.connection.close()
    
    def execute(self, sql):
        return self.cursor.execute(sql)

class CrossDatabaseComparator:
    singlem_db_name = "otus.sqlite3"
    reads_db_name = "reads_db"
    assemblies_db_name = "assemblies_db"
    bins_db_name = "bins_db"

    def __init__(self, **kwargs):
        logging.info("Initialising CrossDatabaseComparator")
        reads_db_path = kwargs.pop('reads_db')
        self.reads_db_path = self._get_singlem_db(reads_db_path)
        assemblies_db_path = kwargs.pop('assemblies_db')
        self.assemblies_db_path = self._get_singlem_db(assemblies_db_path)
        bins_db_path = kwargs.pop('bins_db')
        self.bins_db_path = self._get_singlem_db(bins_db_path)
        self.force = kwargs.pop('force')

        self.output_db_path = kwargs.pop('output_db')
        self.output_path = kwargs.pop('output')

        if self.output_db_path:
            logging.info(f"Creating output database at: {self.output_db_path}")
            self.output_db = self._connect_database(self.output_db_path)
        else:
            logging.info("Creating temporary output database")
            self.output_db = self._connect_database('')
        
        logging.info("Attaching input databases")
        self._attach_database(self.reads_db_path, self.reads_db_name)
        if self.assemblies_db_path:
            self._attach_database(self.assemblies_db_path, self.assemblies_db_name)
        self._attach_database(self.bins_db_path, self.bins_db_name)
    
    def _get_singlem_db(self, db_path):
        if db_path and db_path.endswith("sdb"):
            return os.path.join(db_path, self.singlem_db_name)
        else:
            return db_path
    
    def _connect_database(self, db_path):
        if self.force:
            if os.path.exists(db_path):
                logging.info("Cleaning output database")
                os.remove(db_path)

        db = SqliteDatabase(db_path)
        tables = db.execute("SELECT name FROM sqlite_master;").fetchall()
        if len(tables) > 0:
            raise Exception("Output database not empty. Rerun with --force to remove.")
        return db
    
    def _attach_database(self, db_path, db_name):
        self.output_db.execute(f"ATTACH '{db_path}' as {db_name};")


def main():
    parser = argparse.ArgumentParser(description='Find prevalent OTUs that are not assembled/binned.')
    parser.add_argument('--debug', help='output debug information', action='store_true')
    parser.add_argument('--quiet', action='store_true')
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
    parser.add_argument('--force', help='remove output database before running', action='store_true')

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
    force = getattr(args, 'force', False)

    # print(f"reads: {reads_db_path}, assem: {assemblies_db_path}, bins: {bins_db_path}, output: {output_db_path}, outputcsv: {output_path}")

    CrossDatabaseComparator(
            reads_db=reads_db_path,
            assemblies_db=assemblies_db_path,
            bins_db=bins_db_path,
            output_db=output_db_path,
            output=output_path,
            force=force)


if __name__ == "__main__":
    main()
