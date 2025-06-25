CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.staging (
    "VIN" VARCHAR(100),
    "County" VARCHAR(100),
    "City" VARCHAR(100),
    "State" VARCHAR(2),
    "Postal Code" VARCHAR(10),
    "Model Year" INTEGER,
    "Make" VARCHAR(100),
    "Model" VARCHAR(100),
    "Electric Vehicle Type" VARCHAR(100),
    "Clean Alternative Fuel Vehicle Eligibility" VARCHAR(100),
    "Electric Range" INTEGER,
    "Base MSRP" DECIMAL(10,2),
    "Legislative District" VARCHAR(100),
    "DOL Vehicle ID" BIGINT,
    "Vehicle Location" VARCHAR(100),
    "Electric Utility" TEXT,
    "2020 Census Tract" VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

