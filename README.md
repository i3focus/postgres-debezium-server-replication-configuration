# Tutorial: Sincronização Inicial e Replicação Contínua PostgreSQL com Debezium Server

Este tutorial atualizado fornece um guia passo a passo para sincronizar tabelas de um banco de dados PostgreSQL (VPS Origem) para outro PostgreSQL (VPS Destino), garantindo que os dados fiquem idênticos e, depois, mantendo a replicação contínua em tempo real (CDC) para todas as alterações futuras.

A solução utiliza o **Debezium Server**, que roda em um único contêiner Docker, sem a necessidade de Apache Kafka, o que torna o provisionamento extremamente rápido [1].

## Arquitetura da Solução

1. **VPS de Origem**: PostgreSQL com replicação lógica habilitada (`wal_level = logical`) e captura de identidade completa (`REPLICA IDENTITY FULL`) em todas as tabelas.

1. **VPS de Destino**: PostgreSQL padrão com as mesmas tabelas já criadas (nesse caso, o banco de dados de destino já possui toda a estrutura).

1. **Debezium Server**: Contêiner Docker que:
  - Executa um **Snapshot Inicial** (`snapshot.mode: initial`) para ler todos os dados existentes nas tabelas e sincronizá-los com o destino.
  - Captura **todas as alterações** (INSERT, UPDATE, DELETE) em tempo real via logical replication.
  - Usa **JDBC Sink** para aplicar as mudanças no destino via `upsert` (evitando duplicatas) e `delete` [1].

## Passo a Passo

### Pré-requisitos

- Duas VPSs com PostgreSQL instalado (versão 10 ou superior).

- As tabelas já existem no banco de dados de destino (a estrutura deve ser idêntica à da origem).

- Docker instalado na VPS onde o Debezium Server irá rodar.

### Passo 1: Configurar o PostgreSQL de origem

O PostgreSQL de origem precisa ter a replicação lógica habilitada. Além disso, para garantir que o Debezium capture todas as colunas durante as atualizações e exclusões em todas as tabelas, precisamos ajustar o `REPLICA IDENTITY`.

Conecte-se ao PostgreSQL de origem e execute os seguintes comandos SQL:

```sql
-- 1. Habilitar replicação lógica
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 20; -- Aumentado para suportar muitas tabelas
ALTER SYSTEM SET max_wal_senders = 20;

-- Reinicie o serviço do PostgreSQL na VPS para aplicar as mudanças
-- sudo systemctl restart postgresql
```

Após o reinício, crie um usuário específico para o Debezium:

```sql
-- 2. Criar usuário de replicação
CREATE ROLE dbz WITH LOGIN PASSWORD 'senha_segura_dbz' REPLICATION;
GRANT ALL PRIVILEGES ON DATABASE origem_db TO dbz;
GRANT USAGE ON SCHEMA public TO dbz;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz;
```

**Passo Crítico:** O Debezium precisa ler o Write-Ahead Log (WAL). Por padrão, o PostgreSQL registra apenas a chave primária nos logs de replicação. Para capturar todas as colunas (necessário para atualizações e deleções corretas), devemos alterar o `REPLICA IDENTITY` de todas as tabelas para `FULL`:

```sql
-- 3. Alterar REPLICA IDENTITY para todas as tabelas
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
    LOOP
        EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', r.schemaname, r.tablename);
    END LOOP;
END $$;
```

### Passo 2: Preparar o PostgreSQL de Destino

O banco de destino não precisa de nenhuma configuração especial de replicação lógica. Como você mencionou que as tabelas já existem lá, não precisamos do `schema.evolution=basic`. O Debezium irá inserir, atualizar ou excluir dados nas tabelas existentes.

Certifique-se de que o banco de dados de destino esteja acessível a partir do IP a partir do qual o Debezium Server será executado.

### Passo 3: Criar o arquivo de configuração do Debezium Server

Crie um diretório no seu servidor (ex.: `/opt/debezium/`) e o arquivo de configuração.

Crie o arquivo `/opt/debezium/application.properties` com o seguinte conteúdo:

```
# ======================================================
# Configuração do Sink (Destino - PostgreSQL)
# ======================================================
debezium.sink.type=jdbc
debezium.sink.jdbc.connection.url=jdbc:postgresql://IP_DO_DESTINO:5432/destino_db
debezium.sink.jdbc.connection.username=postgres
debezium.sink.jdbc.connection.password=senha_do_destino
debezium.sink.jdbc.insert.mode=upsert
debezium.sink.jdbc.primary.key.mode=record_key
# Como as tabelas já existem no destino, desativamos a evolução de schema
debezium.sink.jdbc.schema.evolution=none
debezium.sink.jdbc.delete.enabled=true

# ======================================================
# Configuração da Fonte (Origem - PostgreSQL)
# ======================================================
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.database.hostname=IP_DA_ORIGEM
debezium.source.database.port=5432
debezium.source.database.user=dbz
debezium.source.database.password=senha_segura_dbz
debezium.source.database.dbname=origem_db
debezium.source.topic.prefix=migracao

# Usa o plugin nativo de saída lógica do PostgreSQL (não requer instalação extra)
debezium.source.plugin.name=pgoutput

# Modo de snapshot: 'initial' faz uma leitura completa de TODAS as tabelas antes de começar o CDC
debezium.source.snapshot.mode=initial

# Captura todas as tabelas do schema public (não precisamos listar as 50 uma a uma)
debezium.source.schema.include.list=public
# Opcional: Se quiser excluir alguma tabela específica do schema public, use:
# debezium.source.table.exclude.list=public.tabela_excluida
```

**Explicação das propriedades:**

- `debezium.source.snapshot.mode=initial`: Este é o comando que força o Debezium a fazer um "scan" completo em todas as 50 tabelas e enviar os dados para o destino, garantindo que os dois bancos fiquem idênticos (o mesmo "footprint") antes de começar o streaming contínuo.

- `debezium.sink.jdbc.insert.mode=upsert`: Garante que, se o registro já existir no destino, será atualizado; se não existir, será inserido. Isso é crucial durante o snapshot inicial para não falhar caso haja duplicatas.

- `debezium.sink.jdbc.delete.enabled=true`: Propaga as exclusões da origem para o destino.

### Passo 4: Executar o Debezium Server com Docker

Navegue até o diretório `/opt/debezium/` e crie um diretório `data` (necessário para salvar o arquivo de offsets):

```bash
mkdir -p /opt/debezium/data
```

Execute o Debezium Server:

```bash
docker run --name debezium-server -d \
    -v /opt/debezium/application.properties:/debezium/config/application.properties:z \
    -v /opt/debezium/data:/debezium/data:z \
    -p 8080:8080 \
    quay.io/debezium/server:latest
```
ou via arquivo de Docker Compose:

```yaml
services:
  debezium-server:
    image: quay.io/debezium/server:3.6
    container_name: debezium-server
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./application.properties:/debezium/config/application.properties:z
      - ./debezium-data:/debezium/data:z
    environment:
      - QUARKUS_LOG_CONSOLE_FORMAT=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c] (%t) %s%e%n
      - QUARKUS_LOG_CONSOLE_LEVEL=INFO
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/q/health/ready"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 30s

```

### Passo 5: Monitorar a Sincronização Inicial e o CDC

Ao iniciar, você verá nos logs do Docker (`docker logs -f debezium-server`) que o Debezium começará a ler as tabelas (`Snapshot step 4 - Snapshot data for table 'public.nome_tabela'`). Ele enviará os registros para o banco de destino.

Como você tem ~50 tabelas, o processo pode levar algum tempo dependendo do volume de dados. O Debezium processará tabela por tabela.

**Após o snapshot inicial (CDC contínuo):** Assim que o snapshot terminar, o Debezium começará a consumir o Write-Ahead Log (WAL) do PostgreSQL de origem.

Para testar o CDC contínuo em tempo real:

1. No banco de **Origem**: `UPDATE tabela1 SET status = 'ativo' WHERE id = 1;`

1. No banco de **Destino**: `SELECT * FROM tabela1 WHERE id = 1;`

A alteração será refletida instantaneamente.

---

**References**[1]: [https://debezium.io/blog/2026/07/06/kafka-less-migration/](https://debezium.io/blog/2026/07/06/kafka-less-migration/) "Debezium Blog: Building Kafka-Less Data Integration Pipelines with Debezium Server (2026)."

---

# Troubleshooting: Debezium Server PostgreSQL to PostgreSQL (CDC & Snapshot)

Este documento compila os erros mais comuns encontrados durante a configuração do Debezium Server para replicação entre dois bancos PostgreSQL (versão 12), detalhando as causas raízes e as soluções aplicadas.

## 1. Erro no Script de Alteração de REPLICA IDENTITY

**Mensagem de Erro:**

```
SQL Error [42703]: ERROR: record "r" has no field "schemaname"
  Where: SQL statement "SELECT format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', r.schemaname, r.tablename)"
```

**Causa:** O comando `format()` estava tentando acessar o campo `r.schemaname` dentro do loop `FOR`, mas a query inicial do `SELECT` dentro do loop apenas retornava `tablename`. Como o `RECORD` (`r`) não continha o campo `schemaname`, o PL/pgSQL falhava.

**Solução:** Adicionar `schemaname` ao `SELECT` inicial do loop para que o `RECORD` contenha ambos os campos necessários.

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public')
    LOOP
        EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL;', r.schemaname, r.tablename);
        RAISE NOTICE 'REPLICA IDENTITY FULL aplicado em %', r.tablename;
    END LOOP;
END $$;
```

## 2. Tabela Fictícia ou Ausente na `pg_tables`

**Mensagem de Erro:**

```
SQL Error [42P01]: ERROR: relation "public.jhi_authority_aud" does not exist
  Where: SQL statement "ALTER TABLE "public.jhi_authority_aud" REPLICA IDENTITY FULL;"
```

**Causa:** A tabela `pg_tables` listou uma tabela que na verdade não existe mais no banco de dados (pode ser um artefato de uma extensão ou de um schema que foi removido). O loop tentou executar o `ALTER TABLE` em uma tabela inexistente, abortando o script.

**Solução:** Filtrar apenas as tabelas que realmente existem no `information_schema` e adicionar um bloco `BEGIN/EXCEPTION` dentro do loop para que, se uma tabela falhar, o script continue para a próxima sem abortar.

```sql
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
```

## 3. Erro ao Consultar o Status do REPLICA IDENTITY

**Mensagem de Erro:**

```
SQL Error [42703]: ERROR: column "replica_identity" does not exist
```

**Causa:** O campo `replica_identity` não existe na tabela de sistema `pg_tables`. Ele pertence à tabela `pg_class` e seu nome correto é `relreplident`.

**Solução:** Consultar diretamente a `pg_class` e fazer o `JOIN` correto com `pg_namespace`, traduzindo os códigos de identidade (`d`, `n`, `f`, `i`) para texto legível.

```sql
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
  AND c.relkind = 'r'
ORDER BY c.relname;
```

## 4. Usuário sem permissão de REPLICATION

**Mensagem de Erro:**

```
ERROR Postgres roles LOGIN and REPLICATION are not assigned to user: your_user
```

**Causa:** O usuário configurado no Debezium (`your_user` ou `replicator`) possuía permissão de `LOGIN` e era `superuser`, mas não possuía explicitamente a `role` de `REPLICATION`, que é obrigatória para o Debezium consumir o Write-Ahead Log (WAL).

**Solução:** Conceder a role de replicação ao usuário no banco de origem.

```sql
ALTER ROLE your_user WITH REPLICATION;
-- ou
ALTER ROLE replicator WITH REPLICATION;
```

## 5. Erro de Sintaxe ao Criar Publication (PostgreSQL 12 vs 15+)

**Mensagem de Erro:**

```
Creating Publication with statement 'CREATE PUBLICATION dbz_publication FOR ALL TABLES;'
ERROR: syntax error at or near "IN"
```

**Causa:** Por padrão, ou quando configurado com `publication.autocreate.mode=all_tables` ou `filtered` no Debezium 3.6, o Debezium tenta criar a publication com a qualificação de schema (ex.: `FOR TABLE "public"."tabela"`). Essa sintaxe foi introduzida apenas no **PostgreSQL 15**. O banco de origem roda **PostgreSQL 12**, que não suporta essa qualificação com aspas duplas.

**Solução:** Desativar a autocriação automática pelo Debezium e criar a publication manualmente no PostgreSQL 12, listando as tabelas apenas pelo nome (sem o prefixo do schema e sem aspas).

1. Remover a linha `debezium.source.publication.autocreate.mode` do `application.properties`.

1. Criar a publication manualmente no banco de origem:

```sql
CREATE PUBLICATION dbz_publication FOR TABLE
    table1,
    table2,
    table3,
    table4;
    -- (listar todas as tabelas desejadas)
```

## 6. Tentativa de Leitura de Schemas Indesejados (ex: `cron`)

**Mensagem de Erro:**

```
Caused by: org.postgresql.util.PSQLException: ERROR: permission denied for schema cron
```

**Causa:** O Debezium tentou escanear o catálogo do banco de dados e encontrou o schema `cron` (geralmente usado pela extensão `pg_cron`), tentando acessá-lo mesmo que o `schema.include.list` estivesse configurado apenas para `public`. O usuário do Debezium não tinha permissão para esse schema.

**Solução:**

1. Configurar explicitamente a exclusão de schemas no `application.properties`:

   ```
   debezium.source.schema.exclude.list=cron,information_schema,pg_catalog,pg_toast
   ```

1. Revogar o acesso do usuário do Debezium a schemas que não devem ser monitorados:

   ```sql
   REVOKE USAGE ON SCHEMA cron FROM replicator;
   ```

## 7. Erro de Permissão no Arquivo de Offsets (`data/offsets.dat`)

**Mensagem de Erro:**

```
WARN [io.debezium.embedded.async.AsyncEmbeddedEngine] Flush of the offsets failed, canceling the flush.: 
java.util.concurrent.ExecutionException: org.apache.kafka.connect.errors.ConnectException: java.nio.file.AccessDeniedException: data/offsets.dat
```

**Causa:** O Debezium Server rodou com sucesso e começou a exportar dados, mas falhou ao tentar salvar o offset (posição no WAL) no disco. Isso ocorre porque o volume Docker montado para `data/offsets.dat` foi criado pelo usuário `root` no host, mas o processo Java dentro do container roda com um UID diferente (geralmente 1001), não tendo permissão de escrita na pasta.

**Solução:** Ajustar as permissões da pasta de dados no host antes de subir o container:

```bash
mkdir -p ./debezium-data
sudo chown -R 1001:1001 ./debezium-data
```

Ou, como alternativa rápida:

```bash
chmod 777 ./debezium-data
```

---

## Resumo da Configuração Final (`application.properties`)

Após resolver todos os problemas acima, a configuração final que funcionou foi:

```
# SINK (DESTINO)
debezium.sink.type=jdbc
debezium.sink.jdbc.connection.url=jdbc:postgresql://IP_DO_DESTINO:5432/DESTINO_DB
debezium.sink.jdbc.connection.username=USUARIO_DESTINO
debezium.sink.jdbc.connection.password=SENHA_DESTINO
debezium.sink.jdbc.insert.mode=upsert
debezium.sink.jdbc.primary.key.mode=record_key
debezium.sink.jdbc.schema.evolution=none
debezium.sink.jdbc.delete.enabled=true
debezium.sink.jdbc.max.retries=5
debezium.sink.jdbc.retry.interval.ms=5000

# SOURCE (ORIGEM)
debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.database.hostname=backend_postgres
debezium.source.database.port=5432
debezium.source.database.user=replicator
debezium.source.database.password=SENHA_ORIGEM
debezium.source.database.dbname=source_db
debezium.source.topic.prefix=migration
debezium.source.plugin.name=pgoutput
debezium.source.slot.name=debezium_slot
debezium.source.snapshot.mode=initial
debezium.source.schema.include.list=public
debezium.source.schema.exclude.list=cron,information_schema,pg_catalog,pg_toast
debezium.source.publication.name=dbz_publication
debezium.source.heartbeat.interval.ms=60000
debezium.source.heartbeat.topics.regex=.

# TRANSFORMAÇÃO (Remover prefixo do nome da tabela)
debezium.transforms=dropPrefix
debezium.transforms.dropPrefix.type=org.apache.kafka.connect.transforms.RegexRouter
debezium.transforms.dropPrefix.regex=migration.public.(.*)
debezium.transforms.dropPrefix.replacement=$1

# QUARKUS
quarkus.log.console.format=%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%c] (%t) %s%e%n
quarkus.log.console.level=INFO
```
