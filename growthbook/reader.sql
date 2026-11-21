SELECT 'CREATE ROLE growthbook' WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'growthbook')\gexec
ALTER ROLE growthbook WITH LOGIN PASSWORD :'password';
GRANT CONNECT ON DATABASE growthbook TO growthbook;
GRANT USAGE ON SCHEMA public TO growthbook;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO growthbook;
ALTER DEFAULT PRIVILEGES FOR ROLE lakehouse IN SCHEMA public GRANT SELECT ON TABLES TO growthbook;
