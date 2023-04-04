#! /usr/bin/env python
#  stolen from study/mockdata.py
#  Warning:  there isn't a lot of sanity checking if that data is properly structured

import sqlite3
import os

# ------------------------------------------------------------
#  if a new field is added to a table:
#     1. CREATE TABLE needs new one
#     2. create_XXX() needs new INSERT line
#     3. where create_XXX() is called, needs a new one
# ----------------------------------------------------------

header_table = """
CREATE TABLE IF NOT EXISTS header (
    id integer PRIMARY KEY,
    key text ,
    val text ,
    version text
);
"""

alma_table = """
CREATE TABLE IF NOT EXISTS alma (
    id integer PRIMARY KEY,
    obs_id              text,
    observatory         text,
    obsnum              INTEGER,
    instrument          text,
    calibration_status  text,
    target_name         text,
    s_ra                        FLOAT,
    s_dec                       FLOAT,
    gal_longitude               FLOAT,
    gal_latitude                FLOAT,
    s_velocity                  FLOAT,
    frequency                   FLOAT,
    s_resolution                FLOAT,
    t_min                       FLOAT, 
    t_exptime                   FLOAT, 
    pol_states                  TEXT,
    pvw                         FLOAT,
    cont_sensitivity_bandwidth  FLOAT,
    sensitivity_10kms           FLOAT,
    project_abstract    text,
    project_title       text,
    proposal_id         text,
    obs_title           text,
    obs_creator_name    text,
    science_keyword     text,
    scientific_category text,
    proposal_authors    text
);
"""

win_table = """
CREATE TABLE IF NOT EXISTS win (
    id integer PRIMARY KEY,
    a_id INTEGER NOT NULL,
    spw TEXT,
    bandnum INTEGER,
    freqc FLOAT,
    freqw FLOAT,
    vlsr FLOAT,
    nlines INTEGER,
    nsources INTEGER,
    nchan INTEGER,
    peak_w FLOAT,
    rms_w FLOAT,
    bmaj FLOAT,
    bmin FLOAT,
    bpa FLOAT,
    qagrade TEXT,
    fcoverage FLOAT,
    FOREIGN KEY (a_id) REFERENCES alma (id)
);
"""

lines_table = """
CREATE TABLE IF NOT EXISTS lines (
    id integer PRIMARY KEY,
    w_id          INTEGER NOT NULL,
    formula       text NOT NULL,
    transition    text NOT NULL,
    restfreq      FLOAT, 
    vmin          FLOAT,
    vmax          FLOAT,
    mom0flux      FLOAT,
    mom1peak      FLOAT,
    mom2peak      FLOAT,
    FOREIGN KEY (w_id) REFERENCES win (id)
);
"""

sources_table = """
CREATE TABLE IF NOT EXISTS sources (
    id integer PRIMARY KEY,
    w_id INTEGER NOT NULL,
    l_id INTEGER,
    ra FLOAT,
    dec FLOAT,
    peak_s FLOAT,
    flux FLOAT,
    smaj FLOAT,
    smin FLOAT,
    spa FLOAT,
    snr_s FLOAT,
    FOREIGN KEY (w_id) REFERENCES win (id),
    FOREIGN KEY (l_id) REFERENCES lines (id)
);
"""

project_abstract    = "We will do something wonderful"
obs_title           = "The End of the Universe"
science_keyword     = "molecules, galaxies"
scientific_category = "ISM"
proposal_authors    = "George, Paul, Ringo"

class MetaDB(object):
    """
    LMT intermediate search page database.
    Re-uses on the A-W-L-S schema invented for study7
    """
    def __init__(self, db_file, create=False):
        self.db   = db_file
        self.conn = None
        self._created = False

        try:
            if os.path.isfile(db_file):
               self.conn = sqlite3.connect(db_file)
            else:
               if create:
                   # default is to create if it doesn't exists
                   self.conn = sqlite3.connect(db_file)
                   self.create_table(header_table)
                   self.create_table(   alma_table)
                   self.create_table(    win_table)
                   self.create_table(  lines_table)
                   self.create_table(sources_table)
                   self._created = True
               else:
                   msg = f"Cannot create the database connection to {db_file}. Check that the file exists. If not, try create=True"
                   raise Exception(msg)
        except sqlite3.Error:
                raise

            

    def close(self):
        """Close the database connection/file"""
        if self.conn is not None:
            self.conn.close()

    def create_table(self, create_table_sql):
        """
        create a table from the create_table_sql statement
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        #try:
        c = self.conn.cursor()
        c.execute(create_table_sql)


    # @todo deal with multiple key,vals in header.
    #def insert_into_header(self,keyval):
    #    s = str(list(keyval.keys())).replace("'","").strip("[").strip("]")
    #    v = tuple(list(keyval.values()))
    #    sql = f"INSERT INTO header(key,val) VALUES("

    def insert_into(self,table,keyval):
        """insert into a table"""
    
        s = str(list(keyval.keys())).replace("'","").strip("[").strip("]")
        v = tuple(list(keyval.values()))
        #print("S,V ",s,v)
        sql = f"INSERT INTO {table}({s}) VALUES("
        for i in v:
            sql += '?, '
        sql+=")"
        sl = list(sql)
        sl[sql.rindex(",")] = ""
        sql = "".join(sl)
        #print(sql,":",len(sql))
        #print(f"cur.execute({sql},{v})")
        cur = self.conn.cursor()
        cur.execute(sql,v)
        self.conn.commit()

    def query(self,table,item):
        sql = f"SELECT {item} from {table};"
        #print("QUERY IS ",sql)
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def create_header(self, entry):
        """
        Create a new project into the header table
        :param project:
        :return: project id
        """
        sql = ''' INSERT INTO header(key,val)
                              VALUES(?,  ?)'''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.conn.commit()
        return cur.lastrowid

    def create_alma(self, entry):
        """
        Create a new project into the alma table
        :param project:
        :return: project id
        obs_id == member_ous_uid
        """

        sql = ''' INSERT INTO alma(obs_id, target_name, s_ra, s_dec,
frequency, project_abstract, obs_title, science_keyword, scientific_category, proposal_authors)
                            VALUES(?,      ?,           ?,    ?,     ?,   ?,
  ?,  ?, ?, ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, e)
        self.conn.commit()
        return cur.lastrowid

    def create_spw(self, entry):
        """
        Create a new project into the spw table
        :param project:
        :return: project id
        """
        sql = ''' INSERT INTO win(a_id, spw, nlines, nsources, nchan, rms_w,
bmaj, bmin, bpa)
                           VALUES(?,       ?,   ?,      ?,        ?,     ?,   ?,   ?,   ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.conn.commit()
        return cur.lastrowid

    def create_lines(self, entry):
        """
        Create a new project into the lines table
        :param project:
        :return: project id
        """
        sql = ''' INSERT INTO lines(w_id, formula, transition, restfreq, vmin, vmax)
                             VALUES(?,      ?,          ?,        ?,          ?,     ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.conn.commit()
        return cur.lastrowid


    def create_sources(self, entry):
        """
        Create a new project into the sources table
        :param project:
        :return: project id
        """
        sql = ''' INSERT INTO sources(w_id, l_id, ra, dec, flux)
                               VALUES(?,      ?,        ?,  ?,   ?) '''
        cur = self.conn.cursor()
        cur.execute(sql, entry)
        self.conn.commit()
        return cur.lastrowid
