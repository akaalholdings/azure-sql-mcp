/*
  Azure SQL MCP index-history contract v1.

  Manual-only installer. Run it separately as an authorized database
  administrator. The MCP server never runs this file, creates these objects,
  or applies index DDL. The dbatools schema must already exist.

  This script creates exactly two append-only telemetry tables. It does not
  create, alter, or remove database users or roles, and it does not change
  permissions. The MCP runtime uses the signed-in identity's existing effective
  permissions.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

IF SCHEMA_ID(N'dbatools') IS NULL
    THROW 51011, 'Create the dbatools schema before running this installer.', 1;
IF OBJECT_ID(N'dbatools.IndexReviewRun', N'U') IS NOT NULL
    THROW 51012, 'IndexReviewRun already exists; use the approved versioned installation procedure.', 1;
IF OBJECT_ID(N'dbatools.IndexReviewSnapshot', N'U') IS NOT NULL
    THROW 51013, 'IndexReviewSnapshot already exists; use the approved versioned installation procedure.', 1;

CREATE TABLE [dbatools].[IndexReviewRun]
(
    RunId nvarchar(200) NOT NULL CONSTRAINT PK_IndexReviewRun PRIMARY KEY,
    ContractVersion varchar(16) NOT NULL CONSTRAINT CK_IndexReviewRun_ContractVersion CHECK (ContractVersion = '2.3.0'),
    SchemaVersion varchar(32) NOT NULL CONSTRAINT CK_IndexReviewRun_SchemaVersion CHECK (SchemaVersion = 'index-history-v1'),
    CollectorVersion varchar(64) NOT NULL,
    DatabaseName nvarchar(128) NOT NULL,
    DatabaseFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_DatabaseFingerprint CHECK (DatabaseFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DatabaseFingerprint) = 64),
    DatabaseIncarnationFingerprint char(64) NULL CONSTRAINT CK_IndexReviewRun_DatabaseIncarnationFingerprint CHECK (DatabaseIncarnationFingerprint IS NULL OR (DatabaseIncarnationFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DatabaseIncarnationFingerprint) = 64)),
    DatabaseIncarnationIdentity varchar(128) NULL,
    EngineFingerprint char(64) NULL CONSTRAINT CK_IndexReviewRun_EngineFingerprint CHECK (EngineFingerprint IS NULL OR (EngineFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(EngineFingerprint) = 64)),
    EngineIdentity varchar(128) NULL,
    EngineStartTimeUtc datetime2(7) NULL,
    IdempotencyKeyHash char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_IdempotencyKeyHash CHECK (IdempotencyKeyHash NOT LIKE '%[^0-9a-fA-F]%' AND LEN(IdempotencyKeyHash) = 64),
    RequestFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_RequestFingerprint CHECK (RequestFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(RequestFingerprint) = 64),
    ObservedAtUtc datetime2(7) NOT NULL,
    CreatedAtUtc datetime2(7) NOT NULL CONSTRAINT DF_IndexReviewRun_CreatedAtUtc DEFAULT SYSUTCDATETIME(),
    CounterEpochFingerprint char(64) NULL CONSTRAINT CK_IndexReviewRun_CounterEpochFingerprint CHECK (CounterEpochFingerprint IS NULL OR (CounterEpochFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(CounterEpochFingerprint) = 64)),
    InventoryFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_InventoryFingerprint CHECK (InventoryFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(InventoryFingerprint) = 64),
    QueryStoreFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_QueryStoreFingerprint CHECK (QueryStoreFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(QueryStoreFingerprint) = 64),
    QueryStoreState varchar(32) NOT NULL,
    QueryCaptureMode varchar(64) NULL,
    ObservationStartUtc datetime2(7) NULL,
    ObservationEndUtc datetime2(7) NULL,
    CoverageJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewRun_CoverageJson CHECK (ISJSON(CoverageJson) = 1 AND JSON_QUERY(CoverageJson) IS NOT NULL),
    SubjectCount int NOT NULL CONSTRAINT CK_IndexReviewRun_SubjectCount CHECK (SubjectCount >= 0),
    SnapshotSetFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewRun_SnapshotSetFingerprint CHECK (SnapshotSetFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(SnapshotSetFingerprint) = 64),
    QueryStoreJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewRun_QueryStoreJson CHECK (ISJSON(QueryStoreJson) = 1 AND JSON_QUERY(QueryStoreJson) IS NOT NULL),
    CONSTRAINT CK_IndexReviewRun_DatabaseIncarnationIdentity CHECK ((DatabaseIncarnationFingerprint IS NULL AND DatabaseIncarnationIdentity IS NULL) OR (DatabaseIncarnationFingerprint IS NOT NULL AND DatabaseIncarnationIdentity IS NOT NULL)),
    CONSTRAINT CK_IndexReviewRun_EngineEpochIdentity CHECK ((EngineFingerprint IS NULL AND EngineIdentity IS NULL AND EngineStartTimeUtc IS NULL) OR (EngineFingerprint IS NOT NULL AND EngineIdentity IS NOT NULL AND EngineStartTimeUtc IS NOT NULL)),
    CONSTRAINT UQ_IndexReviewRun_Database_Idempotency UNIQUE (DatabaseFingerprint, IdempotencyKeyHash)
);

CREATE TABLE [dbatools].[IndexReviewSnapshot]
(
    SnapshotId nvarchar(200) NOT NULL CONSTRAINT PK_IndexReviewSnapshot PRIMARY KEY,
    RunId nvarchar(200) NOT NULL,
    SubjectId nvarchar(200) NOT NULL,
    SubjectKind varchar(32) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_SubjectKind CHECK (SubjectKind IN ('existing_index', 'missing_index')),
    SubjectFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_SubjectFingerprint CHECK (SubjectFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(SubjectFingerprint) = 64),
    ObjectId bigint NULL,
    IndexId int NULL,
    SchemaName nvarchar(128) NULL,
    ObjectName nvarchar(128) NULL,
    IndexName nvarchar(128) NULL,
    DefinitionJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_DefinitionJson CHECK (ISJSON(DefinitionJson) = 1 AND JSON_QUERY(DefinitionJson) IS NOT NULL),
    DefinitionFingerprint char(64) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_DefinitionFingerprint CHECK (DefinitionFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(DefinitionFingerprint) = 64),
    CounterEpochFingerprint char(64) NULL CONSTRAINT CK_IndexReviewSnapshot_CounterEpochFingerprint CHECK (CounterEpochFingerprint IS NULL OR (CounterEpochFingerprint NOT LIKE '%[^0-9a-fA-F]%' AND LEN(CounterEpochFingerprint) = 64)),
    CountersJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_CountersJson CHECK (ISJSON(CountersJson) = 1 AND JSON_QUERY(CountersJson) IS NOT NULL),
    ObservedAtUtc datetime2(7) NOT NULL,
    FirstObservedAtUtc datetime2(7) NULL,
    LastObservedAtUtc datetime2(7) NULL,
    SizePages bigint NULL CONSTRAINT CK_IndexReviewSnapshot_SizePages CHECK (SizePages IS NULL OR SizePages >= 0),
    SizeBytes bigint NULL CONSTRAINT CK_IndexReviewSnapshot_SizeBytes CHECK (SizeBytes IS NULL OR SizeBytes >= 0),
    WriteBurden bigint NULL CONSTRAINT CK_IndexReviewSnapshot_WriteBurden CHECK (WriteBurden IS NULL OR WriteBurden >= 0),
    QueryStoreReferencesJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_QueryStoreReferencesJson CHECK (ISJSON(QueryStoreReferencesJson) = 1 AND LEFT(LTRIM(QueryStoreReferencesJson), 1) = '['),
    ProtectionsJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_ProtectionsJson CHECK (ISJSON(ProtectionsJson) = 1 AND JSON_QUERY(ProtectionsJson) IS NOT NULL),
    MissingSignatureJson nvarchar(max) NULL CONSTRAINT CK_IndexReviewSnapshot_MissingSignatureJson CHECK (MissingSignatureJson IS NULL OR (ISJSON(MissingSignatureJson) = 1 AND JSON_QUERY(MissingSignatureJson) IS NOT NULL)),
    AggregatesJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_AggregatesJson CHECK (ISJSON(AggregatesJson) = 1 AND JSON_QUERY(AggregatesJson) IS NOT NULL),
    CoverageJson nvarchar(max) NOT NULL CONSTRAINT CK_IndexReviewSnapshot_CoverageJson CHECK (ISJSON(CoverageJson) = 1 AND JSON_QUERY(CoverageJson) IS NOT NULL),
    CONSTRAINT CK_IndexReviewSnapshot_SubjectActionInvariants CHECK (SubjectKind IN ('existing_index', 'missing_index') AND (SubjectKind = 'existing_index' OR MissingSignatureJson IS NOT NULL)),
    CONSTRAINT FK_IndexReviewSnapshot_Run FOREIGN KEY (RunId)
        REFERENCES [dbatools].[IndexReviewRun](RunId),
    CONSTRAINT UQ_IndexReviewSnapshot_Run_Subject UNIQUE (RunId, SubjectId)
);

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
