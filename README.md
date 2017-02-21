Overview
========================

This set of scripts converts as much as possible of a DB2 UDB database. It does not work with DB2 zOS.

First, you'll need a SQL dump of your schema. Use the db2topg script to convert it to a PostgreSQL schema.
You can optionnally ask for a script to dump all the data from this database.

If you want to migrate your data, run this produced script. Il will retrieve CSV files from DB2. Then you'll need 
to import these files into PostgreSQL, thanks to the ``deltocopy.pl`` script. You'll need Perl's Text::CSV_XS module
(packaged in most Perl environments).

Pull requests and issues are very welcome to help me improve this tool.

Steps
========================

First, dump your DB2 schema into a file:

``db2look -d my_database_name -e -l -xd -o my_db2_sql_script``

You'll probably need to add -i and -w parameters :

``-i: User ID to log on to the server where the database resides``

``-w: Password to log on to the server where the database resides``

Then, convert this file using ``db2topg.pl``:

``./db2topg.pl -f my_db2_sql_script -o my_output_dir -d db2_dbname -u db2_user -p db2_pwd``

Give a look at all the warnings produced: some things HAVE to be renamed from DB2 to PostgreSQL, as naming conventions and namespacing differ. The objects you will access by name (sequences, tables…) will be specially highlighted.

You'll get 3 SQL scripts: before.sql, after.sql and unsure.sql.

  * before.sql must be run before importing data. It contains the creation statements of tables, sequences, tablespaces (if you asked for them, which you probably don't want), roles.

  * after.sql must be run after importing data. It contains the creation statements of indexes, and most constraints.

  * unsure.sql contains everything that may have to be converted: views, triggers, check constraints…

So for now, you can run the before.sql, something like:

``psql -e --set=ON_ERROR_STOP=1 --single-transaction -f before.sql my_database``

You should add your connection parameters to this command. You can also use or not the ON_ERROR_STOP and --single-transaction parameters, depending on wether you want the script to stop on error, and cancel everything. This remark holds true for all following psql commands

Now, if you need, you can use the export.db2 script (it should have been created in my_output_dir), on the DB2 server, or a machine with the db2 command line tool, set-up to access the server. Your user will need SELECT access to all the tables, and permission to EXECUTE NULLID.SQLUBH05. Put this script on this machine, and run it this way (replace /path/to/ with the real path, or better yet, put the db2 executable in your path)

``/path/to/db2 -f export.db2``

It will produce a bunch of del/lob files, containing all your database's tables' content.

There is also a ``parallel_unload.pl`` script to try and run several db2 scripts in parallel. This is a bit ugly, as db2 does its best not to accept running more than once per tty. So this script does su to create new ttys and run db2 commands in them. Use at your own peril (and if you have a better way of running several db2 commands at the same time, PLEASE tell me).

Now, you can convert and inject all these into PostgreSQL, using the deltocopy.pl script. For deltocopy, you need Perl::CSV::XS

``deltocopy -d my_output_dir | psql -e --set=ON_ERROR_STOP=1 my_database``

deltocopy produces all the COPY statements to load data into PostgreSQL to its stdout. You may add a -e option to specify encoding.

To get the full list of supported encodings in Perl:

``perl -e 'use Encode; print join("\n",Encode->encodings(":all"));'``

If you are in a hurry, and your server can bear it, there is a parallel mode. I advise you to only use it when you have validated that everything is ok at least once:

``time deltocopy.pl -d my_output_dir -j 12 -o 'psql mydb' 2>&1 | tee import_log`` (time is optional, it's just to measure how fast you go)

Adapt the -o option to your needs, it's the command that each parallel instance will use to output data to PostgreSQL. It must connect to the correct database, with no password.



Then, if your data loading succeeded, you can run the last two scripts:

``psql -e --set=ON_ERROR_STOP=1 --single-transaction  -f after.sql my_database``

This part can also be sped up: most indexes and primary keys can be built simultaneously. You can use https://github.com/marco44/dispatcher_pg to help you parallelize this.

This one may fail if some constraints cannot validate (if a constraint is already invalid in DB2's SQL script, it will be created as not valid in PostgreSQL, so only the valid constraints from DB2 should be enforced in PostgreSQL, and they shouldn't fail).

And last step, try the unsure file (the ALTER TABLE in in can also be run in parallel, so grep them in unsure.sql and run them through dispatcher_pg:

``psql -e -f unsure.sql my_database``

This one will probably fail: it contains everything that cannot be guaranteed to work: SQL views (they embed a SQL statement, and there are a lot of differences between both engines), triggers, check constraint (both can embed SQL statements or even PL code). Triggers and functions will definitely fail.

When you have managed to create every object, don't forget to run a vacuum verbose analyze on the whole database.
