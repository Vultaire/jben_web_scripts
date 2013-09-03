"""Uses VARBINARY and binary encoding and jams UTF-8 into it.

Needed because GoDaddy uses MySQL 5.0, and even if it were using 5.5+,
the various MySQL connectors are in various states of working or
not.

"""

import sys
import gzip
import pprint

import xml.sax
import xml.sax.handler
import xml.sax.xmlreader
from xml.parsers import expat


from xml.sax.expatreader import ExpatParser

class CustomExpatParser(ExpatParser):
    def reset(self):
        ExpatParser.reset(self)
        self._parser.SetParamEntityParsing(expat.XML_PARAM_ENTITY_PARSING_NEVER)


class FirstPassContentHandler(xml.sax.handler.ContentHandler):

    """Purpose: to identify required lengths of data and attributes for each table."""

    def __init__(self, conn):
        xml.sax.handler.ContentHandler.__init__(self)
        self.conn = conn
        self.cursor = conn.cursor()
        self.stack = []
        self.elem_len_d = {}
        self.attr_len_d = {}
        self.fk_needed = set()

    def startDocument(self):
        print "Parsing file to determine data structure..."

    def startElement(self, name, attrs):
        # Track whether we need a foreign key.
        # Since we don't record docroot in the db, we check if the
        # stack has more than just docroot.  (i.e. > 1)
        if len(self.stack) > 1:
            self.fk_needed.add(name)

        self.stack.append("")
        for k, v in attrs.items():
            self.attr_len_d.setdefault(name, {})
            self.attr_len_d[name][k] = max(len(v) * 4, self.attr_len_d[name].get(k, 0))

    def endElement(self, name):
        data = self.stack.pop()
        # Again, not doing anything for docroot, so only do something
        # if docroot is still on the stack.
        if len(self.stack) > 0:
            self.elem_len_d[name] = max(len(data) * 4, self.elem_len_d.get(name, 0))

    def characters(self, content):
        self.stack[-1] = self.stack[-1] + content.strip()

    def endDocument(self):
        print "Creating database tables..."
        try:
            # Create tables... *to do*
            for elem, data_len in self.elem_len_d.iteritems():
                cols = ["id INTEGER AUTO_INCREMENT PRIMARY KEY"]
                if elem in self.fk_needed:
                    cols.append("fk INTEGER")
                if data_len > 0:
                    cols.append("data VARBINARY({0})".format(data_len))  # Need +1...?
                attr_d = self.attr_len_d.get(elem, {})
                for attr, data_len in sorted(attr_d.iteritems()):
                    attr = attr.replace(":", "_")
                    cols.append("{0} VARBINARY({1})".format(attr, data_len))
                query = """\
CREATE TABLE `{0}` (
    {1}
) CHARACTER SET binary;""".format(elem, ",\n    ".join(cols))
                try:
                    self.cursor.execute(query)
                except:
                    print query
                    raise
        finally:
            self.cursor.close()


class ContentHandler(xml.sax.handler.ContentHandler):

    """Purpose: populating the tables"""

    def __init__(self, conn, commit_interval):
        xml.sax.handler.ContentHandler.__init__(self)
        self.conn = conn
        self.cursor = conn.cursor()
        self.stack = []
        self.count = 0
        self.commit_interval = commit_interval

    def startDocument(self):
        print "Parsing file again, this time populating the database..."

    def startElement(self, name, attrs):
        if len(self.stack) == 0:
            # docroot; id is dummy value since we don't create a record for this.
            _id = None
        else:
            cols = []
            vals = []
            fk = self.stack[-1]["id"]
            if fk is not None:  # Parent is JMdict, which we ignore...
                cols.append("fk")
                vals.append(fk)
            for key, value in attrs.items():
                key = key.replace(":", "_")
                cols.append(key)
                vals.append(value.encode('utf-8'))
            cols = ", ".join(cols)
            val_template = ", ".join(["%s" for val in vals])
            query = "INSERT INTO `{0}` ({1}) VALUES ({2});".format(name, cols, val_template)
            try:
                self.cursor.execute(query, vals)
            except:
                print query, vals
                raise
            _id = self.cursor.lastrowid
        self.stack.append({"id": _id, "name": name, "attrs": dict(attrs), "data": "", "children": []})

    def endElement(self, name):
        record = self.stack.pop()
        if len(record["data"]) > 0:
            query = "UPDATE `{0}` SET DATA=%s WHERE ID=%s".format(name)
            try:
                self.cursor.execute(query, (record["data"].encode('utf-8'), record["id"]))
            except:
                print query, (record["data"], record["id"])
                raise
        print "Data:\n{0}".format(pprint.pformat(record))
        if len(self.stack) <= 1:
            self.count += 1
            if self.count % self.commit_interval == 0:
                print "Count: {0}, committing...".format(self.count)
                self.conn.commit()
        else:
            self.stack[-1]["children"].append(record)

    def characters(self, content):
        self.stack[-1]["data"] = self.stack[-1]["data"] + content.strip()

    def endDocument(self):
        print "Final commit...".format(self.count)
        self.conn.commit()
        self.cursor.close()


import MySQLdb
#import mysql.connector

def init_db(conn, db_name):
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        dbs = [row[0] for row in cursor.fetchall()]
        if db_name in dbs:
            print "Dropping existing database..."
            cursor.execute("DROP DATABASE {0}".format(db_name))
        print "Creating new empty database..."
        cursor.execute("CREATE DATABASE {0}".format(db_name))
    finally:
        cursor.close()

def switch_to_db(conn, db_name):
    cursor = conn.cursor()
    try:
        cursor.execute("USE {0}".format(db_name))
    finally:
        cursor.close()

def parse_file(filename, db_name, user, commit_interval, passwd=None):
    infile = gzip.open(filename) if filename.lower().endswith(".gz") else open(filename)
    with infile:
        if passwd is None:
            import getpass
            passwd = getpass.getpass("Enter password: ")
        conn = MySQLdb.connect(user=user, passwd=passwd, use_unicode=False)
        #conn = mysql.connector.connect(user=user, password=passwd, use_unicode=False)
        try:
            #init_db(conn, db_name)
            switch_to_db(conn, db_name)
            reader = xml.sax.make_parser(["CustomExpatParser"])  # Creates ExpatParser instance
            #reader.setContentHandler(FirstPassContentHandler(conn))
            #reader.parse(infile)
            #infile.seek(0)
            reader.setContentHandler(ContentHandler(conn, commit_interval))
            reader.parse(infile)
        finally:
            conn.close()

def parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("filename", help="Path to input dictionary file.  Supports gzipped and uncompressed dictionaries.")
    ap.add_argument("--db-name", default="jmdict",
                    help="Output dictionary.  (Default: %(default)s)")
    ap.add_argument("--user", default="root",
                    help="Database user.  (Default: %(default)s)")
    ap.add_argument("--commit-interval", type=int, default=100,
                    help="Number of records to write between commits.  (Default: %(default)s)")
    return ap.parse_args()

def main():
    args = parse_args()
    parse_file(args.filename, args.db_name, args.user, args.commit_interval)
    return 0

if __name__ == "__main__":
    sys.exit(main())