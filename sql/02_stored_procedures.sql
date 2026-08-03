/*  Stored procedures called by the ADF pipelines  */

CREATE OR ALTER PROCEDURE ctl.usp_GetActiveSources
    @SourceSystem VARCHAR(50) = NULL   -- NULL = every system
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        r.SourceId,
        r.SourceSystem,
        r.SourceType,
        r.SourceSchema,
        r.SourceTable,
        r.LoadType,
        r.WatermarkColumn,
        r.PrimaryKeyColumns,
        r.TargetContainer,
        r.TargetPath,
        r.FileFormat,
        ISNULL(w.LastWatermark, '1900-01-01') AS LastWatermark,
        /* The query ADF's Copy activity will execute. Building it here keeps the
           pipeline JSON free of expression soup.                              */
        CASE
            WHEN r.LoadType = 'Full'
                THEN CONCAT('SELECT * FROM [', r.SourceSchema, '].[', r.SourceTable, ']')
            ELSE CONCAT('SELECT * FROM [', r.SourceSchema, '].[', r.SourceTable, ']',
                        ' WHERE [', r.WatermarkColumn, '] > ''',
                        CONVERT(VARCHAR(23), ISNULL(w.LastWatermark, '1900-01-01'), 126), '''',
                        ' AND [', r.WatermarkColumn, '] <= ''',
                        CONVERT(VARCHAR(23), SYSUTCDATETIME(), 126), '''')
        END AS SourceQuery,
        CONVERT(VARCHAR(23), SYSUTCDATETIME(), 126) AS NewWatermark
    FROM ctl.SourceRegistry r
    LEFT JOIN ctl.WatermarkControl w ON w.SourceId = r.SourceId
    WHERE r.IsActive = 1
      AND (@SourceSystem IS NULL OR r.SourceSystem = @SourceSystem)
    ORDER BY r.LoadPriority, r.SourceId;
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_UpdateWatermark
    @SourceId      INT,
    @NewWatermark  DATETIME2(3),
    @PipelineRunId VARCHAR(60)
AS
BEGIN
    SET NOCOUNT ON;

    /* Only ever move the watermark forward. A clock skew or a re-run with a stale
       value must not cause the next load to re-read months of data.            */
    MERGE ctl.WatermarkControl AS tgt
    USING (SELECT @SourceId AS SourceId) AS src
        ON tgt.SourceId = src.SourceId
    WHEN MATCHED AND @NewWatermark > tgt.LastWatermark THEN
        UPDATE SET LastWatermark    = @NewWatermark,
                   LastSuccessRunId = @PipelineRunId,
                   LastUpdated      = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (SourceId, LastWatermark, LastSuccessRunId)
        VALUES (@SourceId, @NewWatermark, @PipelineRunId);
END
GO

CREATE OR ALTER PROCEDURE ctl.usp_LogPipelineRun
    @PipelineRunId VARCHAR(60),
    @PipelineName  VARCHAR(128),
    @SourceId      INT           = NULL,
    @StartTime     DATETIME2(0),
    @EndTime       DATETIME2(0)  = NULL,
    @RowsRead      BIGINT        = NULL,
    @RowsCopied    BIGINT        = NULL,
    @Status        VARCHAR(20),
    @ErrorMessage  NVARCHAR(MAX) = NULL,
    @WatermarkFrom DATETIME2(3)  = NULL,
    @WatermarkTo   DATETIME2(3)  = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.PipelineAuditLog
        (PipelineRunId, PipelineName, SourceId, StartTime, EndTime,
         RowsRead, RowsCopied, Status, ErrorMessage, WatermarkFrom, WatermarkTo)
    VALUES
        (@PipelineRunId, @PipelineName, @SourceId, @StartTime, @EndTime,
         @RowsRead, @RowsCopied, @Status, @ErrorMessage, @WatermarkFrom, @WatermarkTo);
END
GO

/* Convenience view for the monitoring dashboard */
CREATE OR ALTER VIEW ctl.vw_LastRunPerSource
AS
    SELECT
        r.SourceSystem, r.SourceSchema, r.SourceTable, r.LoadType,
        a.Status, a.StartTime, a.DurationSec, a.RowsCopied, a.ErrorMessage
    FROM ctl.SourceRegistry r
    OUTER APPLY (
        SELECT TOP 1 *
        FROM ctl.PipelineAuditLog l
        WHERE l.SourceId = r.SourceId
        ORDER BY l.StartTime DESC
    ) a
    WHERE r.IsActive = 1;
GO
