===================
 J-Ben Web Scripts
===================

This repository contains scripts used during the writing of J-Ben Web.
They are shared in hopes others may find them useful, but with no
warranty, implied or otherwise.

These scripts, unless otherwise noted, are licensed under the 3-clause
BSD license.  The full text is available in LICENSE.txt.

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
