Configuration
-------------


Server default configuration can be altered by setting environment variable.

=====================   ===================   =========================
 Name                   Default               Description
=====================   ===================   =========================
MASENKO_STORE_DIR       `/tmp`                The directory in which the WAL files are stored.
MASENKO_MAX_WAL_SIZE     25MB                 Maximum size of a WAL in bytes. After reaching the limit, a new WAL file is started. Using a big WAL file avoids often state rewriting (each WAL file must be initialized with the current server state), but requires more time to rebuild the server state during the server start.
MASENKO_LISTEN_TCP      `"localhost:12345"`   Listening address for the client interface.
MASENKO_LISTEN_HTTP     `"localhost:8000"`    Listening address for the Web UI.
MASENKO_HEARTBEAT       `"45s"`               Client heartbeat frequency.
=====================   ===================   =========================
