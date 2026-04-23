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
        },
        "primary_key": ["ProductID"],
        "load_strategy": "incremental_with_soft_delete",
        "watermark_column": "ModifiedDate",
        "soft_delete_column": "DiscontinuedDate",
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
        },
        "primary_key": ["AddressID"],
        "load_strategy": "full_snapshot",
        "watermark_column": "",
        "soft_delete_column": "",
        "initial_watermark": "1900-01-01 00:00:00",
    }

    
}