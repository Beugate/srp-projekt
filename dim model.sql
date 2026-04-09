

DROP DATABASE IF EXISTS dim_model_projekt;
CREATE DATABASE dim_model_projekt CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE dim_model_projekt;


CREATE TABLE Dim_Date (
    DateKey         INT             NOT NULL,
    FullDate        DATE            NOT NULL,
    Year            INT             NOT NULL,
    Quarter         INT             NOT NULL,
    QuarterName     VARCHAR(2)      NOT NULL,
    Month           INT             NOT NULL,
    MonthName       VARCHAR(10)     NOT NULL,
    MonthShort      VARCHAR(3)      NOT NULL,
    Week            INT             NOT NULL,
    DayOfMonth      INT             NOT NULL,
    DayOfWeek       INT             NOT NULL,
    DayName         VARCHAR(10)     NOT NULL,
    IsWeekend       TINYINT(1)      NOT NULL DEFAULT 0,
    CONSTRAINT PK_Dim_Date PRIMARY KEY (DateKey)
);


CREATE TABLE Dim_Product (
    ProductKey      INT             NOT NULL AUTO_INCREMENT,
    ProductID       VARCHAR(20)     NOT NULL,
    ProductName     VARCHAR(200)    NOT NULL,
    SubCategory     VARCHAR(50)     NOT NULL,
    Category        VARCHAR(50)     NOT NULL,
    SupplierName    VARCHAR(150)    NOT NULL DEFAULT 'Unknown',
    CONSTRAINT PK_Dim_Product PRIMARY KEY (ProductKey),
    CONSTRAINT UQ_Dim_Product UNIQUE (ProductID)
);


CREATE TABLE Dim_Customer (
    CustomerKey     INT             NOT NULL AUTO_INCREMENT,
    CustomerID      VARCHAR(20)     NOT NULL,
    CustomerName    VARCHAR(100)    NOT NULL,
    Segment         VARCHAR(20)     NOT NULL,
    ValidFrom       DATE            NOT NULL,
    ValidTo         DATE            NULL,
    IsCurrent       TINYINT(1)      NOT NULL DEFAULT 1,
    CONSTRAINT PK_Dim_Customer PRIMARY KEY (CustomerKey)
);


CREATE TABLE Dim_Location (
    LocationKey     INT             NOT NULL AUTO_INCREMENT,
    City            VARCHAR(100)    NOT NULL,
    State           VARCHAR(100)    NOT NULL,
    Country         VARCHAR(100)    NOT NULL,
    Market          VARCHAR(20)     NOT NULL,
    Region          VARCHAR(50)     NOT NULL,
    PostalCode      VARCHAR(20)     NOT NULL DEFAULT 'N/A',
    CONSTRAINT PK_Dim_Location PRIMARY KEY (LocationKey)
);


CREATE TABLE Dim_ShipMode (
    ShipModeKey     INT             NOT NULL AUTO_INCREMENT,
    ShipMode        VARCHAR(30)     NOT NULL,
    OrderPriority   VARCHAR(15)     NOT NULL,
    CONSTRAINT PK_Dim_ShipMode PRIMARY KEY (ShipModeKey),
    CONSTRAINT UQ_Dim_ShipMode UNIQUE (ShipMode, OrderPriority)
);


CREATE TABLE Fact_Sales (
    SalesKey        INT             NOT NULL AUTO_INCREMENT,
    OrderID         VARCHAR(25)     NOT NULL,
    ProductKey      INT             NOT NULL,
    CustomerKey     INT             NOT NULL,
    LocationKey     INT             NOT NULL,
    ShipModeKey     INT             NOT NULL,
    OrderDateKey    INT             NOT NULL,
    ShipDateKey     INT             NOT NULL,
    Sales           DECIMAL(12,2)   NOT NULL,
    Quantity        INT             NOT NULL,
    Discount        DECIMAL(6,4)    NOT NULL DEFAULT 0,
    Profit          DECIMAL(12,2)   NOT NULL,
    ShippingCost    DECIMAL(10,2)   NOT NULL DEFAULT 0,
    DeliveryDays    INT             NOT NULL DEFAULT 0,
    CONSTRAINT PK_Fact_Sales        PRIMARY KEY (SalesKey),
    CONSTRAINT FK_Fact_Product      FOREIGN KEY (ProductKey)   REFERENCES Dim_Product(ProductKey),
    CONSTRAINT FK_Fact_Customer     FOREIGN KEY (CustomerKey)  REFERENCES Dim_Customer(CustomerKey),
    CONSTRAINT FK_Fact_Location     FOREIGN KEY (LocationKey)  REFERENCES Dim_Location(LocationKey),
    CONSTRAINT FK_Fact_ShipMode     FOREIGN KEY (ShipModeKey)  REFERENCES Dim_ShipMode(ShipModeKey),
    CONSTRAINT FK_Fact_OrderDate    FOREIGN KEY (OrderDateKey) REFERENCES Dim_Date(DateKey),
    CONSTRAINT FK_Fact_ShipDate     FOREIGN KEY (ShipDateKey)  REFERENCES Dim_Date(DateKey)
);