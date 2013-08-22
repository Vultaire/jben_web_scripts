"""Uses Latin-1 encoding and jams UTF-8 into it.

Needed because GoDaddy uses MySQL 5.0, and even if it were using 5.5+,
the various MySQL connectors are in various states of working or
not.

"""

import sys
import gzip

import xml.sax
import xml.sax.handler
import xml.sax.xmlreader


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
                    cols.append("data VARCHAR({0})".format(data_len))  # Need +1...?
                attr_d = self.attr_len_d.get(elem, {})
                for attr, data_len in sorted(attr_d.iteritems()):
                    attr = attr.replace(":", "_")
                    cols.append("{0} VARCHAR({1})".format(attr, data_len))
                query = """\
CREATE TABLE `{0}` (
    {1}
) CHARACTER SET latin1;""".format(elem, ",\n    ".join(cols))
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
            fk = self.stack[-1][0]
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
        self.stack.append([_id, ""])   # (id, data)

    def endElement(self, name):
        _id, data = self.stack.pop()
        if len(data) > 0:
            query = "UPDATE `{0}` SET DATA=%s WHERE ID=%s".format(name)
            try:
                self.cursor.execute(query, (data.encode('utf-8'), _id))
            except:
                print query, (data, _id)
                raise
        if len(self.stack) <= 1:
            self.count += 1
            if self.count % self.commit_interval == 0:
                print "Count: {0}, committing...".format(self.count)
                self.conn.commit()

    def characters(self, content):
        self.stack[-1][-1] = self.stack[-1][-1] + content.strip()

    def endDocument(self):
        print "Final commit...".format(self.count)
        self.conn.commit()
        self.cursor.close()


import MySQLdb
#import mysql.connector

def switch_to_db(conn, db_name):
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW DATABASES")
        dbs = [row[0] for row in cursor.fetchall()]
        if db_name in dbs:
            print "Dropping existing database..."
            cursor.execute("DROP DATABASE {0}".format(db_name))
        print "Creating new empty database..."
        cursor.execute("CREATE DATABASE {0}".format(db_name))
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
            switch_to_db(conn, db_name)
            reader = xml.sax.make_parser()  # Creates ExpatParser instance
            reader.setContentHandler(FirstPassContentHandler(conn))
            reader.parse(infile)
            infile.seek(0)
            reader.setContentHandler(ContentHandler(conn, commit_interval))
            reader.parse(infile)
        finally:
            conn.close()

def parse_args():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("filename")
    ap.add_argument("--db-name", default="jmdict")
    ap.add_argument("--user", default="root")
    ap.add_argument("--commit-interval", default=100)
    return ap.parse_args()

def main():
    args = parse_args()
    parse_file(args.filename, args.db_name, args.user, args.commit_interval)
    return 0

if __name__ == "__main__":
    sys.exit(main())
