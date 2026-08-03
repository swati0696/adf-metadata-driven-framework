/*  Metadata-driven ingestion framework — control schema
    Target: Azure SQL Database                                              */

IF SCHEMA_ID('ctl') IS NULL
    EXEC('CREATE SCHEMA ctl');
GO

/* ---------------------------------------------------------------------------
   SourceRegistry — one row per source object. This table IS the framework.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('ctl.SourceRegistry') IS NULL
CREATE TABLE ctl.SourceRegistry
(
    SourceId          INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystem      VARCHAR(50)   NOT NULL,   -- logical system name, e.g. SalesDB
    SourceType        VARCHAR(20)   NOT NULL    -- AzureSql | RestApi | SharePoint
                      CONSTRAINT CK_SourceType
                      CHECK (SourceType IN ('AzureSql','RestApi','SharePoint')),
    SourceSchema      VARCHAR(50)   NULL,
    SourceTable       VARCHAR(128)  NOT NULL,
    LoadType          VARCHAR(20)   NOT NULL
                      CONSTRAINT CK_LoadType
                      CHECK (LoadType IN ('Full','Incremental')),
    WatermarkColumn   VARCHAR(128)  NULL,       -- required when LoadType = Incremental
    PrimaryKeyColumns VARCHAR(400)  NULL,       -- comma separated, used by the Silver MERGE
    TargetContainer   VARCHAR(100)  NOT NULL,
    TargetPath        VARCHAR(400)  NOT NULL,   -- e.g. salesdb/orders
    FileFormat        VARCHAR(20)   NOT NULL DEFAULT 'Parquet',
    LoadPriority      TINYINT       NOT NULL DEFAULT 5,
    IsActive          BIT           NOT NULL DEFAULT 1,
    CreatedDate       DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate      DATETIME2(0)  NULL,
    CONSTRAINT UQ_SourceRegistry UNIQUE (SourceSystem, SourceSchema, SourceTable)
);
GO

/* Incremental loads must declare a watermark column. Enforce it here rather than
   discovering it at 2am when the pipeline silently loads nothing.                */
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_Watermark_Required')
ALTER TABLE ctl.SourceRegistry ADD CONSTRAINT CK_Watermark_Required
    CHECK (LoadType = 'Full' OR WatermarkColumn IS NOT NULL);
GO

/* ---------------------------------------------------------------------------
   WatermarkControl — high-water mark per source.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('ctl.WatermarkControl') IS NULL
CREATE TABLE ctl.WatermarkControl
(
    SourceId         INT           NOT NULL PRIMARY KEY
                     REFERENCES ctl.SourceRegistry(SourceId),
    LastWatermark    DATETIME2(3)  NOT NULL DEFAULT '1900-01-01',
    LastSuccessRunId VARCHAR(60)   NULL,
    LastUpdated      DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

/* ---------------------------------------------------------------------------
   PipelineAuditLog — every run, successful or not.
   --------------------------------------------------------------------------- */
IF OBJECT_ID('ctl.PipelineAuditLog') IS NULL
CREATE TABLE ctl.PipelineAuditLog
(
    AuditId        BIGINT IDENTITY(1,1) PRIMARY KEY,
    PipelineRunId  VARCHAR(60)   NOT NULL,
    PipelineName   VARCHAR(128)  NOT NULL,
    SourceId       INT           NULL REFERENCES ctl.SourceRegistry(SourceId),
    StartTime      DATETIME2(0)  NOT NULL,
    EndTime        DATETIME2(0)  NULL,
    DurationSec    AS DATEDIFF(SECOND, StartTime, EndTime),
    RowsRead       BIGINT        NULL,
    RowsCopied     BIGINT        NULL,
    Status         VARCHAR(20)   NOT NULL
                   CONSTRAINT CK_AuditStatus
                   CHECK (Status IN ('Started','Succeeded','Failed','Skipped')),
    ErrorMessage   NVARCHAR(MAX) NULL,
    WatermarkFrom  DATETIME2(3)  NULL,
    WatermarkTo    DATETIME2(3)  NULL
);
GO

CREATE NONCLUSTERED INDEX IX_AuditLog_RunId  ON ctl.PipelineAuditLog (PipelineRunId);
CREATE NONCLUSTERED INDEX IX_AuditLog_Status ON ctl.PipelineAuditLog (Status, StartTime DESC);
GO
