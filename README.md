# Tutorial: Sincronização Inicial e Replicação Contínua PostgreSQL (50+ Tabelas) com Debezium Server

Este tutorial atualizado fornece um guia passo a passo para sincronizar tabelas de um banco de dados PostgreSQL (VPS Origem) para outro PostgreSQL (VPS Destino), garantindo que os dados fiquem idênticos e, depois, mantendo a replicação contínua em tempo real (CDC) para todas as alterações futuras.

A solução utiliza o **Debezium Server**, que roda em um único contêiner Docker, sem a necessidade de Apache Kafka, o que torna o provisionamento extremamente rápido [1].

## Arquitetura da Solução

1. **VPS de Origem**: PostgreSQL com replicação lógica habilitada (`wal_level = logical`) e captura de identidade completa (`REPLICA IDENTITY FULL`) em todas as tabelas.

1. **VPS de Destino**: PostgreSQL padrão com as mesmas 50 tabelas já criadas.

1. **Debezium Server**: Contêiner Docker que:
  - Executa um **Snapshot Inicial** (`snapshot.mode: initial`) para ler todos os dados existentes nas 50 tabelas e sincronizá-los com o destino.
  - Captura **todas as alterações** (INSERT, UPDATE, DELETE) em tempo real via logical replication.
  - Usa **JDBC Sink** para aplicar as mudanças no destino via `upsert` (evitando duplicatas) e `delete` [1].

## Passo a Passo

### Pré-requisitos

- Duas VPSs com PostgreSQL instalado (versão 10 ou superior).

- As 50 tabelas já existem no banco de dados de destino (a estrutura deve ser idêntica à origem).

- Docker instalado na VPS onde o Debezium Server irá rodar.

### Passo 1: Configurar o PostgreSQL de origem

O PostgreSQL de origem precisa ter a replicação lógica habilitada. Além disso, para garantir que o Debezium capture todas as colunas durante as atualizações e exclusões em todas as 50 tabelas, precisamos ajustar o `REPLICA IDENTITY`.

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

**Passo Crítico:** O Debezium precisa ler o Write-Ahead Log (WAL). Por padrão, o PostgreSQL registra apenas a chave primária nos logs de replicação. Para capturar todas as colunas (necessário para atualizações e deleções corretas), devemos alterar o `REPLICA IDENTITY` de todas as 50 tabelas para `FULL`:

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

- `debezium.sink.jdbc.insert.mode=upsert`: Garante que, se o registro já existir no destino, ele será atualizado; se não existir, será inserido. Isso é crucial durante o snapshot inicial para não falhar se houver duplicatas.

- `debezium.sink.jdbc.delete.enabled=true`: Propaga as exclusões feitas na origem para o destino.

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
