"""
Library for building and querying a simple SQLite3 database with fits headers.
"""

# Python 2 compatibility
from __future__ import print_function, unicode_literals
try:
    input = raw_input
except NameError:
    pass

import os
from datetime import datetime
import sqlite3
import astropy.io.fits as fits

__version__ = "0.1.0"

# Configuration
conf = {
    # The sqlite storage file
    'dbfile': 'database.db',
    # The directory where the data is stored
    'basedir': '/home/rtr/',
    # The header keywords to index in the database.
    # The inner dict holds SQL datatype (for table creation) and a column name
    'header_keywords': {
        'IMAGETYP': dict(datatype='CHARACTER(8)', name='imagetype'),
        'EXPTIME': dict(datatype='FLOAT', name='exptime'),
        'PROJECT': dict(datatype='CHARACTER(16)', name='project'),
        'OBJECT': dict(datatype='CHARACTER(32)', name='object'),
        'SLIT': dict(datatype='TINYINT', name='slit'),
        'I2POS': dict(datatype='TINYINT', name='i2pos'),
        'IODID': dict(datatype='TINYINT', name='iodid'),
        'DATE-OBS': dict(datatype='DATETIME', name='date'),
    },
}


def _row_to_dict(cursor, row):
    """Produce a dict from a database row"""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class Database:
    def __init__(self, readonly=True):
        """Set readonly=False in order to make changes"""
        self.dbfile = conf['dbfile']
        if readonly and os.path.isfile(self.dbfile):
            # Py2 workaround to open in readonly mode
            self._fd = os.open(self.dbfile, os.O_RDONLY)
            self.connection = sqlite3.connect('/dev/fd/%d' % self._fd)
        else:
            # Open normally (r/w)
            self.connection = sqlite3.connect(self.dbfile)
        self.connection.row_factory = _row_to_dict
        self.cursor = self.connection.cursor()
        self.columns = {'path': 'CHARACTER(72) UNIQUE'}
        for c in conf['header_keywords'].values():
            self.columns[c['name']] = c['datatype']
        # TODO: Check concistency with db structure and warn user if necessary

    def close(self):
        self.connection.close()
        try:
            os.close(self._fd)
        except AttributeError:
            pass

    def sql(self, *args, **kwargs):
        """Run raw sql code"""
        return self.cursor.execute(*args, **kwargs)

    def rebuild_tables(self):
        """Remove and recreate all tables"""
        if self._confirm():
            # Remove old table
            print('Deleting old tables..')
            try:
                self.sql('DROP TABLE files')
            except sqlite3.OperationalError:
                pass
            # Create new table
            print('Recreating tables..')
            column_defs = ['%s %s' % (key, self.columns[key])
                           for key in self.columns]
            query = "CREATE TABLE files (%s)" % ", ".join(column_defs)
            self.sql(query)
            print('DONE!')

    def insert_from_file(self, path):
        """Parse a file and insert it into the table (or replace existing)"""
        # Empty record
        r = {key: '' for key in self.columns}
        # Get file path relative to base dir
        r['path'] = os.path.relpath(path, conf['basedir'])
        with fits.open(path) as h:
            header = h[0].header
            for key, colsettings in conf['header_keywords'].items():
                name = colsettings['name']
                try:
                    if "DATE" in colsettings['datatype']:
                        r[name] = datetime.strptime(
                            header[key], '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        r[name] = header[key]
                except KeyError:
                    r[name] = ''
        # Build and execute query
        keys = list(r.keys())
        query = 'INSERT OR REPLACE INTO files ({}) VALUES ({})'.format(
            ', '.join(keys),
            ', '.join(['?']*len(r))
        )
        self.sql(query, [r[k] for k in keys])
        self.connection.commit()

    def query(self, **kwargs):
        # Assemble SQL query
        clauses = []
        values = []
        for key, val in kwargs.items():
            if key not in self.columns:
                print("Illegal keyword argument: '{}'".format(key))
                print("The following keywords are allowed: {}".format(
                    ", ".join(self.columns)
                ))
                return
            else:
                clauses.append("{} = ?".format(key))
                values.append(val)
        query = "SELECT * FROM files WHERE {}".format(" AND ".join(clauses))
        # Run query and return cursor
        return self.sql(query, values)

    def query_files(self, **kwargs):
        """Wrap self.query() and return a list of files"""
        result = self.query(**kwargs)
        return [row['path'] for row in result]

    def flush(self):
        """Empty all tables"""
        if self._confirm():
            print('Flushing..')
            self.sql("DELETE FROM files")
            self.sql("VACUUM")
            print('DONE!')

    def _confirm(self):
        """Generic confirmation dialog"""
        print('Are you sure? (Y/[N])')
        if input().lower() == 'y':
            return True
        else:
            print('Aborted..')
            return False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
