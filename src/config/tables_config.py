TABLES_CONFIG = {
    "saleslt_product": {
        "source": {
            "schema": "SalesLT",
            "table": "Product",
            "query_columns": [
                "ProductID",
                "Name",
                "ProductNumber",
                "Color",
                "StandardCost",
                "ListPrice",
                "Size",
                "Weight",
                "ProductCategoryID",
                "ProductModelID",
                "SellStartDate",
                "SellEndDate",
                "DiscontinuedDate",
                "rowguid",
                "ModifiedDate",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_product",
            "silver_table": "demo.silver.saleslt_product"
        },
        "primary_key": ["ProductID"],
        "load_strategy": "incremental_with_soft_delete",
        "watermark_column": "ModifiedDate",
        "soft_delete_column": "DiscontinuedDate",
        "initial_watermark": "1900-01-01 00:00:00",
    },
    "saleslt_salesorderheader": {
        "source": {
            "schema": "SalesLT",
            "table": "SalesOrderHeader",
            "query_columns": [
                "SalesOrderID",
                "RevisionNumber",
                "OrderDate",
                "DueDate",
                "ShipDate",
                "Status",
                "OnlineOrderFlag",
                "SalesOrderNumber",
                "PurchaseOrderNumber",
                "AccountNumber",
                "CustomerID",
                "ShipToAddressID",
                "BillToAddressID",
                "ShipMethod",
                "CreditCardApprovalCode",
                "SubTotal",
                "TaxAmt",
                "Freight",
                "TotalDue",
                "Comment",
                "rowguid",
                "ModifiedDate",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_salesorderheader",
            "silver_table": "demo.silver.saleslt_salesorderheader"
        },
        "primary_key": ["SalesOrderID"],
        "load_strategy": "incremental_upsert",
        "watermark_column": "ModifiedDate",
        "soft_delete_column": None,
        "initial_watermark": "1900-01-01 00:00:00",
    },


    "saleslt_address": {
        "source": {
            "schema": "SalesLT",
            "table": "Address",
            "query_columns": [
                "AddressID",
                "AddressLine1",
                "AddressLine2",
                "City",
                "StateProvince",
                "CountryRegion",
                "PostalCode",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_address",
            "silver_table": "demo.silver.saleslt_address"
        },
        "primary_key": ["AddressID"],
        "load_strategy": "full_snapshot",
        "watermark_column": None,
        "soft_delete_column": None,
    },
    "saleslt_customeraddress": {
        "source": {
            "schema": "SalesLT",
            "table": "CustomerAddress",
            "query_columns": [
                "CustomerID",
                "AddressID",
                "AddressType",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_customeraddress",
            "silver_table": "demo.silver.saleslt_customeraddress"
        },
        "primary_key": ["SalesOrderID", "SalesOrderDetailID"],
        "load_strategy": "full_snapshot",
        "watermark_column": None,
        "soft_delete_column": None,
    },

    "saleslt_salesorderdetail_snapshot": {
        "source": {
            "schema": "SalesLT",
            "table": "SalesOrderDetail",
            "query_columns": [
                "SalesOrderID",
                "SalesOrderDetailID",
                "OrderQty",
                "ProductID",
                "UnitPrice",
                "UnitPriceDiscount",
                "LineTotal",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_salesorderdetail_snapshot",
             "silver_table": "demo.silver.saleslt_salesorderdetail"
        },
        "primary_key": ["SalesOrderID", "SalesOrderDetailID"],
        "load_strategy": "full_snapshot",
        "watermark_column": None,
        "soft_delete_column": None,
    },

    "saleslt_salesorderdetail_cdc": {
        "source": {
            "schema": "SalesLT",
            "table": "SalesOrderDetail",
            "query_columns": ["*"],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_salesorderdetail_cdc",
            "silver_table": "demo.silver.saleslt_salesorderdetail"
        },
        "primary_key": ["SalesOrderID", "SalesOrderDetailID"],
        "load_strategy": "incremental_cdc",
        "watermark_column": None,
        "soft_delete_column": None,
        "cdc": {
            "capture_instance": "SalesLT_SalesOrderDetail",
            "row_filter_option": "all",
        },
    },

    "saleslt_customer": {
        "source": {
            "schema": "SalesLT",
            "table": "Customer",
            "query_columns": [
                "CustomerID",
                "NameStyle",
                "Title",
                "FirstName",
                "MiddleName",
                "LastName",
                "Suffix",
                "CompanyName",
                "SalesPerson",
                "EmailAddress",
                "Phone",
                "PasswordHash",
                "PasswordSalt",
                "rowguid",
                "ModifiedDate",
            ],
        },
        "target": {
            "bronze_table": "demo.bronze.saleslt_customer",
            "silver_table": "demo.silver.saleslt_customer",
        },
        "primary_key": ["CustomerID"],
        "load_strategy": "incremental_upsert",
        "watermark_column": "ModifiedDate",
        "soft_delete_column": None,
        "initial_watermark": "1900-01-01 00:00:00",
    },
}