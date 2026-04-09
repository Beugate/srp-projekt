create database projekt;
use projekt;
 
CREATE TABLE Customers (
    CustomerID      VARCHAR(20)     NOT NULL,
    CustomerName    VARCHAR(100)    NOT NULL,
    Segment         VARCHAR(20)     NOT NULL,
    CONSTRAINT PK_Customers PRIMARY KEY (CustomerID)
);
 
CREATE TABLE Locations (
    LocationID      INT             NOT NULL AUTO_INCREMENT,
    City            VARCHAR(100)    NOT NULL,
    State           VARCHAR(100)    NOT NULL,
    Country         VARCHAR(100)    NOT NULL,
    PostalCode      VARCHAR(20)     NULL,
    Region          VARCHAR(50)     NOT NULL,
    Market          VARCHAR(20)     NOT NULL,
    CONSTRAINT PK_Locations PRIMARY KEY (LocationID)
);
 
CREATE TABLE Shippers (
    ShipperID       INT             NOT NULL AUTO_INCREMENT,
    ShipMode        VARCHAR(30)     NOT NULL,
    CONSTRAINT PK_Shippers PRIMARY KEY (ShipperID)
);
 
CREATE TABLE Suppliers (
    SupplierID      INT             NOT NULL AUTO_INCREMENT,
    SupplierName    VARCHAR(150)    NOT NULL,
    CONSTRAINT PK_Suppliers PRIMARY KEY (SupplierID)
);
 
CREATE TABLE Categories (
    CategoryID      INT             NOT NULL AUTO_INCREMENT,
    CategoryName    VARCHAR(50)     NOT NULL,
    SubCategory     VARCHAR(50)     NOT NULL,
    CONSTRAINT PK_Categories PRIMARY KEY (CategoryID)
);
 
CREATE TABLE Products (
    ProductID       VARCHAR(20)     NOT NULL,
    ProductName     VARCHAR(200)    NOT NULL,
    CategoryID      INT             NOT NULL,
    SupplierID      INT             NULL,
    CONSTRAINT PK_Products  PRIMARY KEY (ProductID),
    CONSTRAINT FK_Prod_Cat  FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID),
    CONSTRAINT FK_Prod_Supp FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID)
);
 
CREATE TABLE Orders (
    OrderID         VARCHAR(25)     NOT NULL,
    CustomerID      VARCHAR(20)     NOT NULL,
    LocationID      INT             NOT NULL,
    ShipperID       INT             NOT NULL,
    OrderDate       DATE            NOT NULL,
    ShipDate        DATE            NOT NULL,
    OrderPriority   VARCHAR(15)     NOT NULL,
    CONSTRAINT PK_Orders    PRIMARY KEY (OrderID),
    CONSTRAINT FK_Ord_Cust  FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    CONSTRAINT FK_Ord_Loc   FOREIGN KEY (LocationID) REFERENCES Locations(LocationID),
    CONSTRAINT FK_Ord_Ship  FOREIGN KEY (ShipperID)  REFERENCES Shippers(ShipperID)
);
 
CREATE TABLE Order_Lines (
    LineID          INT             NOT NULL AUTO_INCREMENT,
    OrderID         VARCHAR(25)     NOT NULL,
    ProductID       VARCHAR(20)     NOT NULL,
    Sales           DECIMAL(12,2)   NOT NULL,
    Quantity        INT             NOT NULL,
    Discount        DECIMAL(5,2)    NOT NULL DEFAULT 0,
    Profit          DECIMAL(12,2)   NOT NULL,
    ShippingCost    DECIMAL(10,2)   NOT NULL DEFAULT 0,
    CONSTRAINT PK_OrderLines    PRIMARY KEY (LineID),
    CONSTRAINT FK_Lines_Order   FOREIGN KEY (OrderID)   REFERENCES Orders(OrderID),
    CONSTRAINT FK_Lines_Product FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);