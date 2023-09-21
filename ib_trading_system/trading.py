import pandas as pd
import time
import os
import logging
from datetime import datetime

from . import apps
from . import utils
from . import utils_executionTime as utils_et
from . import utils_order as utils_od

from .apps import log



class Trading:

    logFolder_trading = 'trading'
    logFolder_placedOrders = 'placed_orders'
    logFolder_placedOrdersLog = 'placed_orders_log'
    logFolder_openOrders = 'open_orders'
    logFolder_openOrdersLog = 'open_orders_log'
    logFolder_callback = 'callback'
    logFolder_callbackLog = 'callback_log'

    orders_header = ['ClientId', 'AccountName', 'Symbol', 'SecType', 'Action', 'TotalQuantity', 'OrderType', 'LmtPrice']

    def __init__(self, orders:pd.DataFrame=pd.DataFrame(), port:int=7496, clientId:int=0, logPath='./tradingLog', second_round_orders_sending_time:str='21:28'):
        self.orders = orders
        self.port = port
        self.clientId = clientId
        self.logPath = logPath
        self.second_round_orders_sending_time = second_round_orders_sending_time


    def run(self):

        """
        Executes the entire trading operation.

        The method performs the following steps in sequence:
        1. Initializes log folders to store trading logs and order information.
        2. Checks whether the time to send the second round of orders has passed or not.
        3. Creates a logger instance to log trading activities.
        4. Separates the orders into two rounds: first and second.
        - The first round of orders are set to execute at opening price.
        - The second round of orders are set to execute during regular trading hours.
        5. Places the first round of orders.
        6. Waits for 3 seconds to verify if the first round of orders were successfully sent.
        7. Continuously checks if all the first round of orders have been executed until a specified time.
        8. Once all orders from the first round are executed, the second round of orders are placed.
        9. Waits again, this time until all orders from the second round are executed.
        10. Retrieves execution reports which contain details of executed trades and commissions.
        11. Closes the logger.

        Parameters:
        None.

        Returns:
        None. The method performs operations side-effectually and logs the relevant information.
        """

        self.init_logFolders(root_path=self.logPath)
        utils_et.is_time_expired(execution_time=self.second_round_orders_sending_time, machine_computation_time_protect=20)

        # Create a logger for logging trading-related activities
        main_logger = log.create_logger(path=self.logFolder_trading, fileName='trading', log_mode='save_and_print', root_logger=False)

        # Separate orders into first and second rounds
        firstRound_orders, secondRound_orders = utils_od.separate_orders(self.orders)
        firstRound_orders['Tif'] = ['OPG']*len(firstRound_orders)  # Setting time-in-force to opening price for the first round orders
        secondRound_orders['Tif'] = ['DAY']*len(secondRound_orders)  # Setting time-in-force to regular trading hours for the second round orders
        firstRound_orderInfo_list = firstRound_orders.to_dict(orient='records')
        secondRound_ordersInfo_list = secondRound_orders.to_dict(orient='records')

        main_logger.info(f'The number of first-round orders : {len(firstRound_orderInfo_list)}')
        main_logger.info(f'The number of second-round orders : {len(secondRound_ordersInfo_list)}')
        
        # Placing the first round of orders
        main_logger.info('Placing first round of orders')
        placed_orders_firstRound = self.place_order(firstRound_orderInfo_list)
        main_logger.info(f'The number of the first round of placed orders : {len(placed_orders_firstRound)}')

        # Wait for 3 seconds and check if the first round orders have been successfully sent to IB
        main_logger.info('Waiting for 3 seconds to check if the first round of orders were sent to IB successfully')
        time.sleep(3)
        openOrder, openOrderStatus = self.request_openOrders()
        main_logger.info(f'The number of open orders is : {len(openOrder)}')

        # Wait until the specified time to check if all orders are executed
        main_logger.info(f'Waiting until {self.second_round_orders_sending_time} to check every second if all orders have been executed')
        utils_et.execute_func_at_time(self.wait_until_orders_are_complete, {'check_freq': 1, 'logger': main_logger}, self.second_round_orders_sending_time, machine_computation_time_protect=1)

        # Placing the second round of orders after all first round orders are executed
        main_logger.info('All orders from the first round are complete. Placing second round of orders')
        placed_orders_secondRound = self.place_order(secondRound_ordersInfo_list)

        # Wait until all second round orders are executed
        main_logger.info('Waiting for all orders from the second round to be executed')
        self.wait_until_orders_are_complete(check_freq=1, logger=main_logger)

        # Retrieve execution reports
        main_logger.info('Retrieving execution reports')
        executions, commissions = self.request_callback()

        # Filter the execution and commission data to ensure they correspond to valid order IDs and the specific client ID
        valid_orderIds = placed_orders_firstRound['OrderId'].to_list()+placed_orders_secondRound['OrderId'].to_list()
        executions_filtered, commissions_filtered = self.filter_valid_callback(executions, commissions, self.clientId, valid_orderIds)

        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        executions_filtered.to_excel(os.path.join(self.logFolder_placedOrders, f'{now}_executions_filtered.xlsx'), index=False)
        commissions_filtered.to_excel(os.path.join(self.logFolder_placedOrders, f'{now}_commissions_filtered.xlsx'), index=False)

        # Close the logger
        apps.log.close_logger(main_logger)

        return executions_filtered, commissions_filtered

    def place_order(self, orderInfo_list:list) -> pd.DataFrame:

        """
        Place an order based on the provided order information.
        
        Args:
        - orderInfo_list (list): A list containing order details.
        
        Returns:
        - pd.DataFrame: A dataframe of placed orders.
        
        Functionality:
        This function places an order and saves the placed order details to an excel file. 
        """
        # Call the main function to place the order
        placed_orders = apps.place_order.main(
            orderInfo_list, self.port, self.clientId, logfile_path=self.logFolder_placedOrdersLog, logfile_name='place_order', log_mode='save_only'
        )
        
        # Get the current timestamp
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save the placed order details to an excel file
        placed_orders.to_excel(os.path.join(self.logFolder_placedOrders, f'{now}_placed_orders.xlsx'), index=False)
        
        return placed_orders

    def request_openOrders(self) -> (pd.DataFrame, pd.DataFrame):
        """
        Request for the current open orders.
        
        Returns:
        - tuple:
            - pd.DataFrame: A dataframe of open orders.
            - pd.DataFrame: A dataframe of open order statuses.
        
        Functionality:
        This function retrieves information about open orders and their status and then 
        saves them separately in two excel files.
        """
        # Call the main function to request open orders
        openOrder, openOrderStatus = apps.request_openOrders.main(
            self.port, self.clientId, logfile_path=self.logFolder_openOrdersLog, logfile_name='AllOpenOrders', log_mode='save_only'
        )
        
        # Get the current timestamp
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save the open orders and their statuses to excel files
        openOrder.to_excel(os.path.join(self.logFolder_openOrders, f'{now}_openOrder.xlsx'), index=False)
        openOrderStatus.to_excel(os.path.join(self.logFolder_openOrders, f'{now}_openOrderStatus.xlsx'), index=False)
        
        return openOrder, openOrderStatus

    def request_callback(self) -> (pd.DataFrame, pd.DataFrame):

        """
        Request for the callback details which include executions and commissions.
        
        Returns:
        - tuple:
            - pd.DataFrame: A dataframe of executions.
            - pd.DataFrame: A dataframe of commissions.
        
        Functionality:
        This function retrieves information about the callback including details about 
        executions and commissions, then saves them in two separate excel files.
        """
        # Call the main function to request callback details
        executions, commissions = apps.request_callback.main(
            self.port, self.clientId, logfile_path=self.logFolder_callbackLog, logfile_name='callback', log_mode='save_only'
        )
        
        # Get the current timestamp
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save the executions and commissions details to excel files
        executions.to_excel(os.path.join(self.logFolder_callback, f'{now}_executions.xlsx'), index=False)
        commissions.to_excel(os.path.join(self.logFolder_callback, f'{now}_commissions.xlsx'), index=False)

        return executions, commissions

    def init_logFolders(self, root_path:str):

        self.logFolder_trading = os.path.join(root_path, self.logFolder_trading)
        self.logFolder_placedOrders = os.path.join(root_path, self.logFolder_placedOrders)
        self.logFolder_placedOrdersLog = os.path.join(root_path, self.logFolder_placedOrdersLog)
        self.logFolder_openOrders = os.path.join(root_path, self.logFolder_openOrders)
        self.logFolder_openOrdersLog = os.path.join(root_path, self.logFolder_openOrdersLog)
        self.logFolder_callback = os.path.join(root_path, self.logFolder_callback)
        self.logFolder_callbackLog = os.path.join(root_path, self.logFolder_callbackLog)

        utils.makedirs(self.logFolder_trading)
        utils.makedirs(self.logFolder_placedOrders)
        utils.makedirs(self.logFolder_placedOrdersLog)
        utils.makedirs(self.logFolder_openOrders)
        utils.makedirs(self.logFolder_openOrdersLog)
        utils.makedirs(self.logFolder_callback)
        utils.makedirs(self.logFolder_callbackLog)

    def wait_until_orders_are_complete(self, check_freq:int, logger:logging.Logger):
        '''
        Waits until all orders are fully processed.
        
        After executing, the function checks every `check_freq` seconds to see if all orders are complete.
        It continuously retrieves the current pending orders and waits until all orders are finalized.
        
        Parameters:
        - check_freq (int): Frequency in seconds to check for order completion.
        - logger (logging.Logger): Logger instance to log the status of the orders.
        
        Returns:
        None. Function ends when all orders are complete.
        '''
        
        flag = True
        while flag:
            open_orders, open_order_status = self.request_openOrders()
            if len(open_orders) == 0:
                flag = False
            logger.info(f'There are still {len(open_orders)} orders pending processing')
            time.sleep(check_freq)  # Pause for check_freq seconds


    def filter_valid_callback(self, executions, commissions, valid_clientId, valid_orderIds):

        """
        Filters the execution and commission data to include only valid client and order IDs.
        
        This function ensures that the returned execution and commission data is 
        relevant to the orders placed by filtering out extraneous data. 
        It helps in narrowing down the result to only those trades that are of interest.

        Parameters:
        - executions (pd.DataFrame): The dataframe containing all execution details.
        - commissions (pd.DataFrame): The dataframe containing all commission details.
        - valid_clientId (int): The client ID which we are interested in.
        - valid_orderIds (list): A list of order IDs which we are interested in.

        Returns:
        - tuple:
            - pd.DataFrame: A filtered dataframe of valid executions.
            - pd.DataFrame: A filtered dataframe of valid commissions.
        """
        # Filter executions by clientId and OrderId present in placed_orders
        executions = executions[executions['ClientId'] == valid_clientId]
        valid_orders = executions['OrderId'].isin(valid_orderIds)
        executions = executions[valid_orders].reset_index(drop=True)

        # Filter commissions based on executions
        commissions = commissions.set_index('ExecId').loc[executions['ExecId']].reset_index()

        return executions, commissions



    
