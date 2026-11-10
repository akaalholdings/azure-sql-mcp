/* Synthetic retail schema for the tuning tasks. Every name is invented.
   Target: Azure SQL Database, compatibility level 160, READ_COMMITTED_SNAPSHOT ON,
   Query Store READ_WRITE, collation SQL_Latin1_General_CP1_CI_AS.
   All foreign keys are created WITH CHECK and are trusted.
   Each task lists the tables it needs in schema.tables and the data shape in data_notes. */

CREATE SCHEMA qe;
GO

CREATE TABLE qe.Regions (
    RegionID    tinyint      NOT NULL CONSTRAINT PK_Regions PRIMARY KEY CLUSTERED,
    RegionCode  char(3)      NOT NULL CONSTRAINT UQ_Regions_Code UNIQUE,
    RegionName  varchar(50)  NOT NULL
);

CREATE TABLE qe.Customers (
    CustomerID           int           IDENTITY(1,1) NOT NULL CONSTRAINT PK_Customers PRIMARY KEY CLUSTERED,
    AccountNumber        varchar(20)   NOT NULL,           -- 'AC' + 10 digits
    LegacyRef            varchar(9)    NULL,               -- number from the old billing system, stored as text
    Email                varchar(254)  NULL,
    FirstName            nvarchar(50)  NOT NULL,
    LastName             nvarchar(80)  NOT NULL,
    RegionID             tinyint       NOT NULL CONSTRAINT FK_Customers_Regions REFERENCES qe.Regions (RegionID),
    Tier                 char(1)       NOT NULL CONSTRAINT DF_Customers_Tier DEFAULT ('S'),   -- S=standard, G=gold, P=partner
    ReferredByCustomerID int           NULL CONSTRAINT FK_Customers_ReferredBy REFERENCES qe.Customers (CustomerID),
    IsActive             bit           NOT NULL CONSTRAINT DF_Customers_IsActive DEFAULT (1),
    CreatedAt            datetime2(3)  NOT NULL,
    CONSTRAINT CK_Customers_Tier CHECK (Tier IN ('S','G','P')),
    CONSTRAINT CK_Customers_AccountNumber CHECK (AccountNumber LIKE 'AC[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'),
    -- digits only, first digit 1-9: no sign, no spaces, no leading zero, never empty
    CONSTRAINT CK_Customers_LegacyRef CHECK (LegacyRef LIKE '[1-9]%' AND LegacyRef NOT LIKE '%[^0-9]%')
);
CREATE UNIQUE NONCLUSTERED INDEX UX_Customers_AccountNumber ON qe.Customers (AccountNumber);
CREATE NONCLUSTERED INDEX IX_Customers_LegacyRef ON qe.Customers (LegacyRef);
CREATE NONCLUSTERED INDEX IX_Customers_Email    ON qe.Customers (Email);
CREATE NONCLUSTERED INDEX IX_Customers_LastName ON qe.Customers (LastName, FirstName);
CREATE NONCLUSTERED INDEX IX_Customers_RegionID ON qe.Customers (RegionID) INCLUDE (Tier, IsActive);
-- note: no index on ReferredByCustomerID, no index on Tier

CREATE TABLE qe.Products (
    ProductID      int            NOT NULL CONSTRAINT PK_Products PRIMARY KEY CLUSTERED,
    SKU            varchar(30)    NOT NULL,
    ProductName    nvarchar(200)  NOT NULL,
    CategoryID     smallint       NOT NULL,
    ListPrice      decimal(19,4)  NOT NULL,
    IsDiscontinued bit            NOT NULL CONSTRAINT DF_Products_IsDiscontinued DEFAULT (0)
);
CREATE UNIQUE NONCLUSTERED INDEX UX_Products_SKU ON qe.Products (SKU);

CREATE TABLE qe.Orders (
    OrderID       bigint         IDENTITY(1,1) NOT NULL CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED,
    CustomerID    int            NOT NULL CONSTRAINT FK_Orders_Customers REFERENCES qe.Customers (CustomerID),
    OrderDate     datetime2(7)   NOT NULL,
    Status        varchar(12)    NOT NULL,   -- Pending, Paid, Shipped, Cancelled, Returned
    SalesChannel  varchar(10)    NOT NULL,   -- Web, Store, Phone, Partner
    PromoCode     varchar(20)    NULL,
    ShipRegionID  tinyint        NULL CONSTRAINT FK_Orders_Regions REFERENCES qe.Regions (RegionID),
    TotalAmount   decimal(19,4)  NOT NULL,
    ModifiedAt    datetime2(3)   NOT NULL
);
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID ON qe.Orders (CustomerID);
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate  ON qe.Orders (OrderDate) INCLUDE (CustomerID, Status, TotalAmount);
CREATE NONCLUSTERED INDEX IX_Orders_Status     ON qe.Orders (Status);
-- note: no index on PromoCode, no index on ShipRegionID

CREATE TABLE qe.OrderLines (
    OrderID     bigint         NOT NULL CONSTRAINT FK_OrderLines_Orders REFERENCES qe.Orders (OrderID),
    LineNumber  smallint       NOT NULL,
    ProductID   int            NOT NULL CONSTRAINT FK_OrderLines_Products REFERENCES qe.Products (ProductID),
    Quantity    int            NOT NULL,
    UnitPrice   decimal(19,4)  NOT NULL,
    Discount    decimal(5,4)   NOT NULL CONSTRAINT DF_OrderLines_Discount DEFAULT (0),
    CONSTRAINT PK_OrderLines PRIMARY KEY CLUSTERED (OrderID, LineNumber)
);
CREATE NONCLUSTERED INDEX IX_OrderLines_ProductID ON qe.OrderLines (ProductID);

CREATE TABLE qe.Payments (
    PaymentID     bigint         IDENTITY(1,1) NOT NULL CONSTRAINT PK_Payments PRIMARY KEY CLUSTERED,
    OrderID       bigint         NOT NULL CONSTRAINT FK_Payments_Orders REFERENCES qe.Orders (OrderID),
    PaymentMethod varchar(10)    NOT NULL,   -- Card, PayPal, Invoice, GiftCard
    Amount        decimal(19,4)  NOT NULL,   -- negative for refunds
    Status        varchar(10)    NOT NULL,   -- Pending, Settled, Failed, Refunded
    PaidAt        datetime2(3)   NOT NULL,
    ExternalRef   nvarchar(64)   NULL        -- payment-provider transaction id
);
CREATE NONCLUSTERED INDEX IX_Payments_OrderID ON qe.Payments (OrderID);
-- note: no index on ExternalRef

CREATE TABLE qe.Events (
    EventID     bigint            IDENTITY(1,1) NOT NULL CONSTRAINT PK_Events PRIMARY KEY CLUSTERED,
    EventTime   datetime2(7)      NOT NULL,
    CustomerID  int               NULL,
    EventType   varchar(30)       NOT NULL,
    SessionID   uniqueidentifier  NULL,
    Payload     nvarchar(max)     NULL
);
CREATE NONCLUSTERED INDEX IX_Events_EventTime ON qe.Events (EventTime);
CREATE NONCLUSTERED INDEX IX_Events_Customer  ON qe.Events (CustomerID, EventTime) INCLUDE (EventType);

CREATE TABLE qe.EmailSuppressions (
    SuppressionID int            IDENTITY(1,1) NOT NULL CONSTRAINT PK_EmailSuppressions PRIMARY KEY CLUSTERED,
    Email         nvarchar(320)  NULL,       -- loaded nightly from the email vendor's file; the loader does not validate
    Reason        varchar(20)    NOT NULL,
    SuppressedAt  datetime2(3)   NOT NULL
);
-- note: no nonclustered indexes

CREATE TABLE qe.TestAccounts (
    CustomerID int          NOT NULL CONSTRAINT PK_TestAccounts PRIMARY KEY CLUSTERED
                            CONSTRAINT FK_TestAccounts_Customers REFERENCES qe.Customers (CustomerID),
    Note       varchar(100) NULL
);

CREATE TABLE qe.BlockedPromoCodes (
    PromoCode  varchar(20)   NOT NULL CONSTRAINT PK_BlockedPromoCodes PRIMARY KEY CLUSTERED,
    BlockedAt  datetime2(3)  NOT NULL
);

-- Columnstore fact table: loaded once a day in SaleDate order.
CREATE TABLE qe.FactSales (
    SaleDate   date           NOT NULL,
    StoreID    int            NOT NULL,
    ProductID  int            NOT NULL,
    Quantity   int            NOT NULL,
    NetAmount  decimal(19,4)  NOT NULL,
    INDEX CCI_FactSales CLUSTERED COLUMNSTORE
);

-- Partitioned archive: one partition per quarter, 2019 Q1 to 2026 Q4 (32 partitions).
CREATE PARTITION FUNCTION pf_EventArchiveQuarter (datetime2(7)) AS RANGE RIGHT FOR VALUES (
    '2019-04-01', '2019-07-01', '2019-10-01',
    '2020-01-01', '2020-04-01', '2020-07-01', '2020-10-01',
    '2021-01-01', '2021-04-01', '2021-07-01', '2021-10-01',
    '2022-01-01', '2022-04-01', '2022-07-01', '2022-10-01',
    '2023-01-01', '2023-04-01', '2023-07-01', '2023-10-01',
    '2024-01-01', '2024-04-01', '2024-07-01', '2024-10-01',
    '2025-01-01', '2025-04-01', '2025-07-01', '2025-10-01',
    '2026-01-01', '2026-04-01', '2026-07-01', '2026-10-01'
);
CREATE PARTITION SCHEME ps_EventArchiveQuarter AS PARTITION pf_EventArchiveQuarter ALL TO ([PRIMARY]);

CREATE TABLE qe.EventArchive (
    EventID     bigint        NOT NULL,
    EventTime   datetime2(7)  NOT NULL,
    CustomerID  int           NOT NULL,
    EventType   varchar(30)   NOT NULL,
    CONSTRAINT PK_EventArchive PRIMARY KEY CLUSTERED (EventTime, EventID) ON ps_EventArchiveQuarter (EventTime)
);
CREATE NONCLUSTERED INDEX IX_EventArchive_Customer ON qe.EventArchive (CustomerID, EventTime)
    ON ps_EventArchiveQuarter (EventTime);
GO

-- Called once per row by two tasks. Neither call is inlined: see each task's prompt.
CREATE FUNCTION qe.fn_CustomerNetPaid12m (@CustomerID int)
RETURNS decimal(19,2)
AS
BEGIN
    DECLARE @v decimal(19,2);
    SELECT @v = SUM(p.Amount)
    FROM qe.Payments p
    JOIN qe.Orders o ON o.OrderID = p.OrderID
    WHERE o.CustomerID = @CustomerID
      AND p.Status IN ('Settled','Refunded')
      AND p.PaidAt >= DATEADD(month, -12, SYSDATETIME());
    RETURN ISNULL(@v, 0);
END;
GO

CREATE FUNCTION qe.fn_NetLineAmount (@Quantity int, @UnitPrice decimal(19,4), @Discount decimal(5,4))
RETURNS decimal(19,2)
AS
BEGIN
    RETURN @Quantity * @UnitPrice * (1 - @Discount);
END;
GO
