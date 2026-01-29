from typing import List, Dict, Any
from db import connection 

cursor = connection.cursor()


def get_customers_by_credit_limit_range():
    """Return customers with credit limits outside the normal range."""
    cursor.execute("""
        select customers.customerName, customers.creditLimit
        from customers 
        where customers.creditLimit < 10000 or customers.creditLimit > 100000
    """)
    query = cursor.fetchall()
    return query

def get_orders_with_null_comments():
    """Return orders that have null comments."""
    cursor.execute(
        """
            select 
                orders.orderNumber, 
                orders.comments 
            from orders 
            where orders.comments is null
            order by orders.orderDate 
        """
    )
    query = cursor.fetchall()
    return query

def get_first_5_customers():
    """Return the first 5 customers."""
    cursor.execute(
        """
            select 
                customers.customerName,
                customers.contactLastName,
                customers.contactFirstName 
            from customers
            order by customers.customerName 
            limit 5
        """
    )
    query = cursor.fetchall()
    return query
def get_payments_total_and_average():
    """Return total and average payment amounts."""
    cursor.execute(
        """
            select 
	            sum(payments.amount) as total,
                avg(payments.amount) as avg,
                min(payments.amount) as min,
                max(payments.amount) as max
            from payments
        """
    )
    query = cursor.fetchall()
    return query

def get_employees_with_office_phone():
    """Return employees with their office phone numbers."""
    cursor.execute(
        """
            select 
	            employees.firstName,
                employees.lastName,
                offices.phone
            from employees
            inner join offices 
            on offices.officeCode = employees.officeCode
        """
    )
    query = cursor.fetchall()
    return query

def get_customers_with_shipping_dates():
    """Return customers with their order shipping dates."""
    cursor.execute(
        """
            select 
                customers.customerName, 
                orders.orderDate 
            from customers 
            inner join orders 
            on customers.customerNumber = orders.customerNumber
        """
    )
    query = cursor.fetchall()
    return query

def get_customer_quantity_per_order():
    """Return customer name and quantity for each order."""
    cursor.execute(
        """
            select 
                customers.customerName, 
                orderdetails.quantityOrdered
            from customers 
            inner join orders on customers.customerNumber = orders.customerNumber
            inner join orderdetails on orders.orderNumber = orderdetails.orderNumber
            order by customers.customerName
        """
    )
    query = cursor.fetchall()
    return query

def get_customers_payments_by_lastname_pattern(pattern: str = "son"):
    """Return customers and payments for last names matching pattern."""
    cursor.execute(
        """
            select 
                customers.customerName, 
                concat(employees.firstName, employees.lastName) as 'full name',
                sum(payments.amount) as 'total amount'
            from employees 
            inner join customers on customers.salesRepEmployeeNumber = employees.reportsTo
            inner join payments on payments.customerNumber = customers.customerNumber 
            where 
                customers.contactFirstName like '%ly%' 
                or customers.contactFirstName like '%Mu%'
            group by 
                customers.customerName, 
                concat(employees.firstName, employees.lastName) 
            order by sum(payments.amount) desc
        """
    )
    query = cursor.fetchall()
    return query
