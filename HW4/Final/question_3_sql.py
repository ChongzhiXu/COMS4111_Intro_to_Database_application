import mysql_check
import pandas as pd


def question_3_example_get_customers(country):
    """
    This is an example of what your answers should look like.
    :param country:
    :return:
    """

    # You may need to set the connection info. I do just to be safe.
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    # You will provide your SQL queries in this format. %s is a parameter.
    q = """
        select customerNumber, customerName, country
            from classicmodels.customers
            where
            country = %s
    """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q, [country])
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


def question_3_revenue_by_country():

    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    # You will provide your SQL queries in this format. %s is a parameter.
    q = """
with rev_orderdetail as
    (select orderNumber, productCode, priceEach*quantityOrdered as revenue, orderLineNumber from classicmodels.orderdetails),
sum_rev_orderdetail as
    (select orderNumber, productCode, sum(revenue) as revenue, orderLineNumber from rev_orderdetail group by orderNumber),
rev_order as
    (select orderNumber, productCode, revenue, status, customerNumber from sum_rev_orderdetail join classicmodels.orders using(orderNumber) where status = 'Shipped'),
cust_rev_order as
    (select customerNumber, orderNumber, revenue, country from rev_order join classicmodels.customers using(customerNumber))
select sum(revenue) as revenue, country from cust_rev_order group by country
    """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result



def question_3_purchases_and_payments():
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    # You will provide your SQL queries in this format. %s is a parameter.
    q = """
with sum_rev_orderdetail as
    (select orderNumber, productCode, sum(revenue1) as revenue, orderLineNumber from
    (select orderNumber, productCode, priceEach*quantityOrdered as revenue1, orderLineNumber from classicmodels.orderdetails) as b
    group by orderNumber),
rev_order as
    (select orderNumber, productCode, revenue, customerNumber from sum_rev_orderdetail join classicmodels.orders using(orderNumber)),
cust_rev_order as
    (select customerNumber, customerName, orderNumber, sum(revenue) as total_spent from classicmodels.customers left join rev_order using(customerNumber) group by customerNumber),
pay_cust as
    (select customerNumber, checkNumber, sum(amount) as total_payments from classicmodels.payments group by customerNumber)
select customerNumber, customerName, ifnull(total_spent,0) as total_spent,
       ifnull(total_payments,0) as total_payments,
       ifnull(total_spent-total_payments,0) as total_unpaid
from cust_rev_order left join pay_cust using(customerNumber) order by customerName
        """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result



def question_3_customers_and_lines():
    mysql_check.set_connect_info("root", "Zyj7725869", "localhost")

    # You will provide your SQL queries in this format. %s is a parameter.
    q = """
with prod_orderdt as
    (select productCode, productLine, orderNumber from classicmodels.products join classicmodels.orderdetails using(productCode)),
prod_ord as
    (select orderNumber, productCode, productLine, customerNumber from prod_orderdt join classicmodels.orders using(orderNumber)),
cust_prod_ord as
    (select customerNumber, customerName, productLine from prod_ord join classicmodels.customers using(customerNumber))
select customerNumber, customerName from classicmodels.customers where customerNumber not in
                                                             (select customerNumber from cust_prod_ord where productLine = 'Planes' or productLine = 'Trucks and Buses')
group by customerNumber
            """

    # Connect and run the query
    conn = mysql_check.get_connection()
    cur = conn.cursor()
    res = cur.execute(q)
    result = cur.fetchall()

    # Convert to a Data Frame and return
    result = pd.DataFrame(result)

    return result


