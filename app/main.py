from fastapi import FastAPI
from db_init import init_database
import uvicorn

app = FastAPI()

init_database()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/q1/customers-credit-limit-outliers")
def customers_credit_limit_outliers():
    pass

@app.get("/q2/orders-null-comments")
def orders_null_comments():
    pass

@app.get("/q3/customers-first-5")
def customers_first_5():
    pass

@app.get("/q4/payments-total-average")
def payments_total_average():
    pass

@app.get("/q5/employees-office-phone")
def employees_office_phone():
    pass

@app.get("/q6/customers-shipping-dates")
def customers_shipping_dates():
    pass

@app.get("/q7/customer-quantity-per-order")
def customer_quantity_per_order():
    pass

@app.get("/q8/customers-payments-by-lastname-pattern")
def customers_payments_by_lastname_pattern(pattern: str = "son"):
    pass


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=3306)
