CREATE OR REPLACE TABLE demo.gold.dim_customer
USING iceberg
AS
with customer as (   
SELECT 
CustomerID,
Title,
FirstName,
MiddleName,
LastName,
CompanyName,
EmailAddress,
Phone
FROM demo.silver.saleslt_customer
where is_current=1),

main_office_address as (
select
ca.CustomerID,
ca.AddressID,
a.AddressLine1 as Address,
a.City,
a.StateProvince as State,
a.CountryRegion as Country,
a.PostalCode
from
demo.silver.saleslt_customeraddress as ca
join
demo.silver.saleslt_address as a
on ca.AddressID=a.AddressID
where ca.AddressType='Main Office'
),
shipping_address as (
select
ca.CustomerID,
ca.AddressID,
a.AddressLine1 as Address,
a.City,
a.StateProvince as State,
a.CountryRegion as Country,
a.PostalCode
from
demo.silver.saleslt_customeraddress as ca
join
demo.silver.saleslt_address as a
on ca.AddressID=a.AddressID
where ca.AddressType='Shipping'
)    

select 
c.CustomerID,
c.Title,
c.FirstName,
c.MiddleName,
c.LastName,
CASE
WHEN c.MiddleName is null THEN
CONCAT(FirstName,' ',LastName)
ELSE
CONCAT(FirstName,' ',MiddleName,' ',LastName)
END as FullName,
c.CompanyName,
c.EmailAddress,
c.Phone,
ma.Address as OfficeAddress,
ma.City as OfficeCity,
ma.State as OfficeState,
ma.Country as OfficeCountry,
ma.PostalCode as OfficePostalCode,
sh.Address as ShippingAddress,
sh.City as ShippingCity,
sh.State as ShippingState,
sh.Country as ShippingCountry,
sh.PostalCode as ShippingPostalCode
from 
customer as c
left join main_office_address as ma
on c.CustomerID=ma.CustomerID
left join shipping_address as sh
on c.CustomerID=sh.CustomerID
order by CustomerID 