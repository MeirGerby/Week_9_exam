from fastapi import FastAPI
from db_init import init_database
import dal
import uvicorn

app = FastAPI()

init_database()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/q1/customers-credit-limit-outliers")
def customers_credit_limit_outliers():
    return dal.get_customers_by_credit_limit_range()

@app.get("/q2/orders-null-comments")
def orders_null_comments():
    return dal.get_orders_with_null_comments()

@app.get("/q3/customers-first-5")
def customers_first_5():
    return dal.get_first_5_customers()

@app.get("/q4/payments-total-average")
def payments_total_average():
    return dal.get_payments_total_and_average()

@app.get("/q5/employees-office-phone")
def employees_office_phone():
    return dal.get_employees_with_office_phone()

@app.get("/q6/customers-shipping-dates")
def customers_shipping_dates():
    return dal.get_customers_with_shipping_dates()

@app.get("/q7/customer-quantity-per-order")
def customer_quantity_per_order():
    return dal.get_customer_quantity_per_order()

@app.get("/q8/customers-payments-by-lastname-pattern")
def customers_payments_by_lastname_pattern(pattern: str = "son"):
    return dal.get_customers_payments_by_lastname_pattern()


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=3306)
