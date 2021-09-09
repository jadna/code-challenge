from extract_postgre import execution as step_one
from save_database import execution as step_two
from save_database import query_orders as query_order
import sys

if __name__ == '__main__':

    step_one()
    step_two()
    query_order()
