import mysql_check
import pandas as pd

def schema_operation_1():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
    create schema classicmodels_star;
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def schema_operation_2():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create table classicmodels_star.Product_type
(
	productCode varchar(128) not null,
	scale varchar(128) null,
	productLine varchar(128) null,
	constraint Product_type_pk
		primary key (productCode)
)
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res




def schema_operation_3():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """

create table classicmodels_star.Location
(
	customerNumber int not null,
	city varchar(128) null,
	country varchar(128) null,
	region varchar(128) null,
	constraint Location_pk
		primary key (customerNumber)
)
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def schema_operation_4():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create table classicmodels_star.date
(
    orderDate date                                                                 not null
        primary key,
    month     enum ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12') null,
    quarter   enum ('1', '2', '3', '4')                                            null,
    year      int                                                                  null
)"""

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def schema_operation_5():
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
create table classicmodels_star.fact
(
    customerNumber int          not null,
    orderDate      date         not null,
    productCode    varchar(128) not null,
    quantity       varchar(128) null,

    constraint fact_date_orderDate_fk
        foreign key (orderDate) references classicmodels_star.date (orderDate),
    constraint fact_location_customerNumber_fk
        foreign key (customerNumber) references classicmodels_star.location (customerNumber)
)"""

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)

    return res


def data_transformation_1():

    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q1 = """
insert into classicmodels_star.date
select distinct orderDate, month(orderDate) as month, quarter(orderDate) as quarter, year(orderDate) as year
from classicmodels.orders"""

    q2 = """
    insert into classicmodels_star.product_type
select productCode, productScale, productLine from classicmodels.products
    """

    q3 = """
    insert into classicmodels_star.location
select customerNumber, city, trim(country) as country, country from classicmodels.customers"""

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res1 = cur.execute(q1)
    res2 = cur.execute(q2)
    res3 = cur.execute(q3)

    return res1
    return res2
    return res3



def data_transformation_2():
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q1 = """
insert into classicmodels_star.fact
select customerNumber, orderDate, productCode, (priceEach*quantityOrdered)
from classicmodels.orders join classicmodels.orderdetails using(orderNumber)
    """

    q2 = """
    update classicmodels_star.location
set region = 'NA' where country = 'USA' or country = 'Canada'
    """

    q3 = """
    update classicmodels_star.location set region = 'AP'
where country = 'Philippines' or country = 'Hong Kong' or country = 'Singapore' or country = 'Japan'
or country = 'Australia' or country = 'New Zealand'
    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res1 = cur.execute(q1)
    res2 = cur.execute(q2)
    res3 = cur.execute(q3)

    return res1
    return res2
    return res3



def data_transformation_3():
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q1 = """
update classicmodels_star.location set region = 'EMEA'
where region != 'NA' and region != 'AP'
    """

    q2 = """

    """

    q3 = """

    """

    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res1 = cur.execute(q1)
    #res2 = cur.execute(q2)
    #res3 = cur.execute(q3)

    return res1
    #return res2
    #return res3

def sales_by_year_region():
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
with fd as
    (select * from classicmodels_star.fact join classicmodels_star.date using(orderDate))
select sum(quantity) as sales, region, year from fd join classicmodels_star.location using(customerNumber)
group by region, year
        """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def sales_by_quarter_year_county_region():
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
with fd as
    (select * from classicmodels_star.fact join classicmodels_star.date using(orderDate))
select sum(quantity) as sales, quarter, year, country, region from fd join classicmodels_star.location using(customerNumber)
group by quarter, year, country, region
        """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def sales_by_product_line_scale_year():
    res = None

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    q = """
with fd as
    (select * from classicmodels_star.fact join classicmodels_star.date using(orderDate))
select sum(quantity) as sales, productLine, scale as productScale, year from fd join classicmodels_star.product_type using(productCode)
group by productLine, productScale, year
        """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result

