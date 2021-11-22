
CREATE USER catalyst WITH PASSWORD 'dbcp';
--CREATE USER tableau_reader WITH ENCRYPTED PASSWORD 'cornishsquatdrubbing';
--CREATE USER tableau_political WITH ENCRYPTED PASSWORD 'bumpyrubbishmandolin';

GRANT ALL ON SCHEMA dbcp TO catalyst;
GRANT ALL ON ALL TABLES IN SCHEMA dbcp TO catalyst;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbcp TO catalyst;

--GRANT ALL ON SCHEMA dbcp_poliical TO catalyst;
