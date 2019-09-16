SET session_replication_role = replica;   /* Used to import without getting FK check errors   */

CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS plpgsql;
CREATE EXTENSION IF NOT EXISTS sslinfo;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
