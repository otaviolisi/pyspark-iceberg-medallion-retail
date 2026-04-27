CREATE OR REPLACE TABLE demo.gold.dim_customer
USING iceberg
AS
select
sh.SalesOrderID,
sh.OrderDate,
sh.DueDate,
sh.ShipDate,
CASE Status
        WHEN 1 THEN 'In Process'
        WHEN 2 THEN 'Approved'
        WHEN 3 THEN 'Backordered'
        WHEN 4 THEN 'Rejected'
        WHEN 5 THEN 'Shipped'
        WHEN 6 THEN 'Cancelled'
        ELSE 'Unknown'
END AS status_description,
so.SalesOrderDetailID,
so.OrderQty,
so.ProductID,
so.UnitPrice,
so.UnitPriceDiscount,
so.LineTotal,
sh.OnlineOrderFlag,
sh.SalesOrderNumber,
sh.CustomerID,
sh.ShipToAddressID,
COALESCE(a.AddressLine1,a.AddressLine2) as ShipAddress,
a.City as ShipCity,
a.StateProvince as ShipState,
a.CountryRegion as ShipCountry,
a.PostalCode as ShipPostalCode,
COALESCE(b.AddressLine1,b.AddressLine2) as BillToAddress,
b.City as BillCity,
b.StateProvince as BillState,
b.CountryRegion as BillCountry,
b.PostalCode as BillPostal,
sh.ShipMethod
from demo.silver.saleslt_salesorderheader  sh
left join demo.silver.saleslt_address a
on sh.ShipToAddressID = a.AddressID
left join demo.silver.saleslt_address b
on sh.BillToAddressID = b.AddressID
join demo.silver.saleslt_salesorderdetail so
on sh.SalesOrderID=so.SalesOrderID