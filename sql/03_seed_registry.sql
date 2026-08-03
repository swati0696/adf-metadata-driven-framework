/*  Example source entries. Onboarding a new table is an INSERT here — nothing else.  */

INSERT INTO ctl.SourceRegistry
    (SourceSystem, SourceType, SourceSchema, SourceTable, LoadType,
     WatermarkColumn, PrimaryKeyColumns, TargetContainer, TargetPath, LoadPriority)
VALUES
    ('SalesDB', 'AzureSql', 'dbo', 'Orders',      'Incremental', 'ModifiedDate', 'OrderId',    'bronze', 'salesdb/orders',      1),
    ('SalesDB', 'AzureSql', 'dbo', 'OrderLines',  'Incremental', 'ModifiedDate', 'OrderLineId','bronze', 'salesdb/orderlines',  1),
    ('SalesDB', 'AzureSql', 'dbo', 'Customers',   'Incremental', 'ModifiedDate', 'CustomerId', 'bronze', 'salesdb/customers',   2),
    ('SalesDB', 'AzureSql', 'dbo', 'Products',    'Full',         NULL,          'ProductId',  'bronze', 'salesdb/products',    3),
    ('SalesDB', 'AzureSql', 'dbo', 'Regions',     'Full',         NULL,          'RegionId',   'bronze', 'salesdb/regions',     3);
GO

INSERT INTO ctl.WatermarkControl (SourceId, LastWatermark)
SELECT SourceId, '1900-01-01'
FROM ctl.SourceRegistry r
WHERE NOT EXISTS (SELECT 1 FROM ctl.WatermarkControl w WHERE w.SourceId = r.SourceId);
GO

SELECT * FROM ctl.SourceRegistry;
