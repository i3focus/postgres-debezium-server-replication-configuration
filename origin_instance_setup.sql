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
    FOR r IN (
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_name = tablename
                AND table_type = 'BASE TABLE'
          )
    )
    LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', r.schemaname, r.tablename);
            RAISE NOTICE 'REPLICA IDENTITY FULL aplicado em %', r.tablename;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'ERRO ao aplicar em %: %', r.tablename, SQLERRM;
        END;
    END LOOP;
END $$;

-- 4. Verificar se todas as tabelas estão configuradas corretamente
SELECT
    n.nspname AS schemaname,
    c.relname AS tablename,
    CASE c.relreplident
        WHEN 'd' THEN 'DEFAULT'
        WHEN 'n' THEN 'NOTHING'
        WHEN 'f' THEN 'FULL'
        WHEN 'i' THEN 'INDEX'
    END AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'   -- 'r' = tabela regular (exclui views)
ORDER BY c.relname;
