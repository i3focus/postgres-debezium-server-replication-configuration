-- ====================================================================
-- SETUP INICIAL DO POSTGRESQL DE ORIGEM
-- Execute este script no banco de dados de ORIGEM como superuser (postgres)
-- ====================================================================

-- 1. Habilitar replicação lógica
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 20;
ALTER SYSTEM SET max_wal_senders = 20;
-- Reinicie o PostgreSQL após esta alteração:
-- sudo systemctl restart postgresql

-- 2. Criar usuário de replicação para o Debezium
CREATE ROLE dbz WITH LOGIN PASSWORD 'your_password' REPLICATION;
GRANT ALL PRIVILEGES ON DATABASE origem_db TO dbz;
GRANT USAGE ON SCHEMA public TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO dbz;

-- 3. ALTERAR REPLICA IDENTITY FULL em TODAS as tabelas do schema public
-- Isso é OBRIGATÓRIO: sem isso, o Debezium só captura a PK nos UPDATEs e DELETEs
-- e não consegue sincronizar corretamente os dados
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
    LOOP
        EXECUTE format('ALTER TABLE %I REPLICA IDENTITY FULL;', 'public.' || r.tablename);
        RAISE NOTICE 'REPLICA IDENTITY FULL aplicado em %', r.tablename;
    END LOOP;
END $$;

-- 4. Verificar se todas as tabelas estão configuradas corretamente
SELECT schemaname, tablename, replica_identity FROM pg_tables
  JOIN pg_class ON relname = tablename
  JOIN pg_namespace ON pg_namespace.nspname = schemaname
    WHERE schemaname = 'public'
      ORDER BY tablename;
