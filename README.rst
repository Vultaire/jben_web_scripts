===================
 J-Ben Web Scripts
===================

This repository contains scripts used during the writing of J-Ben Web.
They are shared in hopes others may find them useful, but with no
warranty, implied or otherwise.

These scripts, unless otherwise noted, are licensed under the 3-clause
BSD license.  The full text is available in LICENSE.txt.

parse_dict_binary.py
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

Top-level elements get an auto-generated JSON attribute which
represents the record as a whole.

Two helper tables are created: _parent, which shows relations between
child and parent elements; and _entity, an optional table created if
any XML entities are encountered during parsing.  (JMdict uses this,
KANJIDIC2 currently does not.)

XML entities are deliberately *not* expanded; this allows for a more
compact database and for applications to more easily search for
content containing these tags.  Again, entities are stored in the
_entity table, so you can substitute at the application level where/if
you like.

In general, indices are *not* precreated; this is up to the user of
the script to add afterwards.  These scripts merely get the database
in a queryable format; optimizing it for your app is up to you.
