CREATE OR REPLACE TABLE demo.gold.dim_customer
USING iceberg
AS
select
ProductID,
Name,
ProductNumber,
Color,
StandardCost,
ListPrice,
`Size`,
Weight,
SellStartDate,
SellEndDate,
DiscontinuedDate
from demo.silver.saleslt_product