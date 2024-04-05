CREATE TABLE "DimTime" (
    "TimeID" SERIAL PRIMARY KEY,
    "Date" DATE NOT NULL,
    "Month" INTEGER NOT NULL,
    "Quarter" INTEGER NOT NULL,
    "Year" INTEGER NOT NULL,
    "Weekday" INTEGER NOT NULL,
    "Hour" INTEGER NOT NULL
);

CREATE TABLE "DimGeography" (
    "GeoID" SERIAL PRIMARY KEY,
    "Country" VARCHAR(255) NOT NULL,
    "Region" VARCHAR(255),
    "State" VARCHAR(255),
    "City" VARCHAR(255)
);

CREATE TABLE "DimProduct" (
    "StockCode" VARCHAR(255) PRIMARY KEY,
    "Description" TEXT NOT NULL,
    "Category" VARCHAR(255)
);

CREATE TABLE "DimCustomer" (
    "CustomerID" INTEGER PRIMARY KEY,
    "FirstName" VARCHAR(255),
    "LastName" VARCHAR(255),
    "Country" VARCHAR(255) NOT NULL
);

CREATE TABLE "FactSales" (
    "SalesID" SERIAL PRIMARY KEY,
    "InvoiceNo" INTEGER NOT NULL,
    "StockCode" VARCHAR(255) NOT NULL,
    "TimeID" INTEGER NOT NULL,
    "CustomerID" INTEGER NOT NULL,
    "Quantity" BIGINT NOT NULL,
    "UnitPrice" NUMERIC NOT NULL,
    "TotalPrice" NUMERIC NOT NULL,
    FOREIGN KEY ("StockCode") REFERENCES "DimProduct" ("StockCode"),
    FOREIGN KEY ("TimeID") REFERENCES "DimTime" ("TimeID"),
    FOREIGN KEY ("CustomerID") REFERENCES "DimCustomer" ("CustomerID")
);
