-- Core banking system-of-record schema for the `bank` database (Azure SQL Edge).
--
-- Idempotent by construction: every object is guarded with an
-- `IF NOT EXISTS` check, so re-running this script against an
-- already-provisioned database is a safe no-op. Batches are separated by a
-- bare `GO` line (sqlcmd/SSMS convention); `src.bank.db.run_script` splits on
-- that marker and executes each batch in order, since CREATE TABLE cannot
-- share a batch with a preceding CREATE SCHEMA.
--
-- Table map:
--   bank.customers            one row per TabFormer `User`
--   bank.accounts             one row per customer (single account per customer, v1)
--   bank.cards                one row per (User, Card) pair; card_token MUST match
--                             the salted-SHA256 token produced by
--                             src.pipeline.ingestion.to_event (imported, not
--                             reimplemented, by src.bank.seed)
--   bank.card_transactions    OLTP system-of-record: one row per authorization,
--                             written by the ticket-11 src.bank.txn_writer replay
--                             (contract-v1 shaped); CDC-enabled (src.bank.cdc) so
--                             Debezium can capture inserts as the change feed
--   bank.scored_transactions  insert-heavy audit log written by the ticket-07 scorer loop
--   bank.fraud_alerts         analyst-facing alert queue written by the scorer,
--                             updated (status) by the ticket-08 dashboard

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bank')
BEGIN
    EXEC('CREATE SCHEMA bank');
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'customers'
)
BEGIN
    CREATE TABLE bank.customers (
        customer_id   NVARCHAR(64)  NOT NULL PRIMARY KEY,
        name          NVARCHAR(200) NOT NULL,
        email         NVARCHAR(200) NOT NULL,
        created_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        risk_tier     NVARCHAR(20)  NOT NULL DEFAULT 'standard'
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'accounts'
)
BEGIN
    CREATE TABLE bank.accounts (
        account_id    NVARCHAR(64)   NOT NULL PRIMARY KEY,
        customer_id   NVARCHAR(64)   NOT NULL,
        opened_at     DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        credit_limit  DECIMAL(12, 2) NOT NULL,
        status        NVARCHAR(20)   NOT NULL DEFAULT 'active',
        CONSTRAINT fk_accounts_customer FOREIGN KEY (customer_id)
            REFERENCES bank.customers (customer_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'cards'
)
BEGIN
    CREATE TABLE bank.cards (
        card_token  CHAR(64)     NOT NULL PRIMARY KEY,
        account_id  NVARCHAR(64) NOT NULL,
        card_type   NVARCHAR(20) NOT NULL,
        issued_at   DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_cards_account FOREIGN KEY (account_id)
            REFERENCES bank.accounts (account_id)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'card_transactions'
)
BEGIN
    CREATE TABLE bank.card_transactions (
        event_id          NVARCHAR(64)   NOT NULL PRIMARY KEY,
        schema_version    NVARCHAR(16)   NOT NULL,
        card_token        CHAR(64)       NOT NULL,
        user_id           NVARCHAR(64)   NOT NULL,
        event_time        DATETIME2      NOT NULL,
        amount            DECIMAL(12, 2) NOT NULL,
        currency          CHAR(3)        NOT NULL,
        channel           NVARCHAR(20)   NOT NULL,
        merchant_name     NVARCHAR(200)  NULL,
        merchant_city     NVARCHAR(100)  NULL,
        merchant_state    NVARCHAR(100)  NULL,
        merchant_country  CHAR(2)        NULL,
        zip               NVARCHAR(10)   NULL,
        mcc               INT            NULL,
        errors            NVARCHAR(200)  NULL,
        is_fraud          BIT            NULL,
        inserted_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_card_transactions_inserted_at'
      AND object_id = OBJECT_ID('bank.card_transactions')
)
BEGIN
    CREATE INDEX ix_card_transactions_inserted_at
        ON bank.card_transactions (inserted_at);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'scored_transactions'
)
BEGIN
    CREATE TABLE bank.scored_transactions (
        event_id           NVARCHAR(64)   NOT NULL PRIMARY KEY,
        card_token          CHAR(64)       NOT NULL,
        event_time          DATETIME2      NOT NULL,
        amount              DECIMAL(12, 2) NOT NULL,
        merchant_name       NVARCHAR(200)  NULL,
        merchant_city       NVARCHAR(100)  NULL,
        merchant_state      NVARCHAR(100)  NULL,
        mcc                 INT            NULL,
        channel             NVARCHAR(20)   NULL,
        fraud_probability   FLOAT          NOT NULL,
        decision            NVARCHAR(20)   NOT NULL,
        cold_card           BIT            NOT NULL DEFAULT 0,
        latency_ms          FLOAT          NULL,
        scored_at           DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_scored_transactions_scored_at'
      AND object_id = OBJECT_ID('bank.scored_transactions')
)
BEGIN
    CREATE INDEX ix_scored_transactions_scored_at
        ON bank.scored_transactions (scored_at);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_scored_transactions_card_token'
      AND object_id = OBJECT_ID('bank.scored_transactions')
)
BEGIN
    CREATE INDEX ix_scored_transactions_card_token
        ON bank.scored_transactions (card_token);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'bank' AND t.name = 'fraud_alerts'
)
BEGIN
    CREATE TABLE bank.fraud_alerts (
        alert_id           INT            NOT NULL IDENTITY(1, 1) PRIMARY KEY,
        event_id            NVARCHAR(64)   NOT NULL,
        card_token          CHAR(64)       NOT NULL,
        fraud_probability   FLOAT          NOT NULL,
        amount              DECIMAL(12, 2) NOT NULL,
        merchant_name       NVARCHAR(200)  NULL,
        created_at          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        status              NVARCHAR(20)   NOT NULL DEFAULT 'open'
            CHECK (status IN ('open', 'confirmed_fraud', 'dismissed')),
        reviewed_at         DATETIME2      NULL
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ix_fraud_alerts_status_created_at'
      AND object_id = OBJECT_ID('bank.fraud_alerts')
)
BEGIN
    CREATE INDEX ix_fraud_alerts_status_created_at
        ON bank.fraud_alerts (status, created_at);
END
GO
