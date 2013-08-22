===================
 J-Ben Web Scripts
===================

parse_dict_latin1.py
====================

This script parses either JMdict or KANJIDIC2 and generates a
MySQL/MariaDB database.  It works via a two-pass procedure, first
scanning the XML document structure and second populating the
database.

XML element content is stored as data, attributes as columns of the
same name (with ":" substituted with "_").  The "fk" column links
children to parents.  No record is generated for the document root;
the immediate children of the docroot are the top-level entries of the
generated database.

No indices are precreated; this is up to the user of the script to add
afterwards.  These scripts merely get the database in a queryable
format; optimizing it for your app is up to you.
