
/usr/local/pgsql/14.5/bin/initdb -D /var/lib/pgsql/14.5/data --username="postgres" --pwfile=/var/lib/pgsql/14.5/passwd

echo "host all all all trust" >> /var/lib/pgsql/14.5/data/pg_hba.conf
echo "listen_addresses = '*'" >> /var/lib/pgsql/14.5/data/postgresql.conf

sed -i 's/max_wal_size = 1GB/max_wal_size = 50GB/g' /var/lib/pgsql/14.5/data/postgresql.conf
sed -i 's/shared_buffers = 128MB/shared_buffers = 32GB/g' /var/lib/pgsql/14.5/data/postgresql.conf
sed -i 's/#work_mem = 4MB/work_mem = 4GB/g' /var/lib/pgsql/14.5/data/postgresql.conf

echo "geqo = off" >> /var/lib/pgsql/14.5/data/postgresql.conf
echo "join_collapse_limit = 100" >> /var/lib/pgsql/14.5/data/postgresql.conf


/usr/local/pgsql/14.5/bin/pg_ctl -D /var/lib/pgsql/14.5/data start

echo "Restoring query-optimization db"
/usr/local/pgsql/14.5/bin/createdb query-optimization
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d query-optimization /home/jgmp/data/backups/query-optimization.backup

echo "Restoring imdb_ceb"
/usr/local/pgsql/14.5/bin/createdb imdb_ceb
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d imdb_ceb /home/jgmp/data/backups/imdb_ceb.backup

echo "Restoring imdb_schema"
/usr/local/pgsql/14.5/bin/createdb imdb_schema
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d imdb_schema /home/jgmp/data/backups/imdb_schema.backup

echo "Restoring stats-ceb"
/usr/local/pgsql/14.5/bin/createdb stats-ceb
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d stats-ceb /home/jgmp/data/backups/stats-ceb.backup

echo "Restoring dsb"
/usr/local/pgsql/14.5/bin/createdb dsb
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d dsb /home/jgmp/data/backups/dsb.backup

echo "Restoring dsb_schema"
/usr/local/pgsql/14.5/bin/createdb dsb_schema
/usr/local/pgsql/14.5/bin/pg_restore --no-privileges --no-owner -d dsb_schema /home/jgmp/data/backups/dsb_schema.backup

/usr/local/pgsql/14.5/bin/pg_ctl -D /var/lib/pgsql/14.5/data stop
