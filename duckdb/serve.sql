-- Startup script for the persistent DuckDB "quack" server (see ./start).
-- Loaded via `duckdb hits.db -init serve.sql`: the server opens hits.db, starts
-- the quack client/server protocol, then blocks reading stdin (a fifo held open
-- by ./start) so the process — and the background quack server — stay up until
-- ./stop kills them.
LOAD quack;
CALL quack_serve('quack:localhost', token = 'clickbench');
