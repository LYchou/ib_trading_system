
import pandas as pd
import os

from ib_trading_system.trading import Trading
from ib_trading_system import utils_callback
from ib_trading_system import utils_time
from ib_trading_system.apps import log




def main(orders):

    current_logger = log.create_logger(path='', fileName='', log_mode='print_only', root_logger=False)

    # connect info
    port = 7497
    clientId = 0

    # place order info
    OrderType = 'LIMIT'
    LmtPrice_percent = 0.1
    second_round_orders_sending_time = '12:37' 

    # dictionary info
    log_path = './log_trading' # log record with ib


    current_logger.info('--- Infomation Check ---')
    current_logger.info(f'port : {port}')
    current_logger.info(f'clientId : {clientId}')
    current_logger.info(f'OrderType : {OrderType}')
    current_logger.info(f'LmtPrice_percent : {LmtPrice_percent}')
    current_logger.info(f'second_round_orders_sending_time : {second_round_orders_sending_time}')
    current_logger.info('--- Dictionary Check ---')
    current_logger.info(f'log_path : {log_path}')
    current_logger.info('---')
    current_logger.info(f'If all the information is correct, please press enter...')
    input('')

    current_logger.info(f'len(orders) : {len(orders)}')

    for order in orders.to_dict(orient='records'):
        current_logger.info(order)

    current_logger.info(f'If all orders are correct, please press enter...')
    input('')


    trading = Trading(orders, port, clientId, logPath=log_path, second_round_orders_sending_time=second_round_orders_sending_time)
    executions, commissions = trading.run()

    datewise_dict = utils_callback.group_execution_and_commission_by_date(executions, commissions, valid_clientId=clientId)

    if datewise_dict!={}:
        lastest_date = max(datewise_dict.keys())
        current_logger.info(f'Get the lastest date of callback. The date of callback is : {lastest_date}')
        if lastest_date==utils_time.now('US/Eastern').date():
            executions, commissions = datewise_dict[lastest_date]

            # Assigned by TWS or Gateway as a unique identifier for each order.
            # PermId remains unchanged regardless of whether an order is fully executed, partially executed, or split into multiple executions.
            # For an order split into multiple executions, all related execution reports share the same PermId, but each will have a unique ExecId.
            # In short, ExecId identifies the execution, while PermId identifies the order.
            executions = executions.drop_duplicates(subset=["ExecId"], keep="last")
            commissions = commissions.drop_duplicates(subset=["ExecId"], keep="last")
            
            current_logger.info(f'len(executions) : {len(executions)}')
            current_logger.info(f'len(commissions) : {len(commissions)}')


if __name__ == '__main__':

    orders = pd.read_excel('sample_orders.xlsx')
    main(orders)
