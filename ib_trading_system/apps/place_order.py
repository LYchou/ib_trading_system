from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import numpy as np
import pandas as pd
import time
from . import log

def main(orderInfo_list:list, port:int=7497, clientId:int=0, logfile_path:str='./', logfile_name='placeOrder', log_mode:str="save_and_print") -> None:
    """
    Main function to place orders using the App class.
    
    Parameters:
    - orderInfo_list (list): List of order information dictionaries.
    - port (int): Port number to connect to. Default is 7497.
    - clientId (int): Client ID for connection. Default is 0.
    - logfile_path (str): Path to the log file. Default is './'.
    - logfile_name (str): Name of the log file. Default is 'placeOrder'.
    - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
    
    Returns:
    - None: This function returns None.
    """
    if orderInfo_list:
        app = App(orderInfo_list, logfile_path, logfile_name, log_mode)
        app.connect('127.0.0.1', port, clientId)
        app.run()
        placed_orders = app.get_placed_orders()
    else:
        placed_orders = pd.DataFrame(columns=App.record_header)

    return placed_orders

class App(EWrapper, EClient):
    """
    A class that encapsulates the functionality for placing orders using Interactive Brokers' TWS API.
    """
    # Example structure of orderInfo_list for clarity.
    orderInfo_list_example = [
        {
            'AccountName':'',
            'Symbol':'AMD',
            'SecType':'STK',
            'Action':'BUY',
            'TotalQuantity':100,
            'OrderType':'MARKET',
            'Tif':'OPG',
            'LmtPrice':0,
        },
    ]

    record_header = ['ClientId', 'OrderId', 'Account', 'Symbol', 'SecType', 'Currency', 'Exchange', 'PrimaryExchange', 'Action', 'TotalQuantity', 'OrderType', 'Tif', 'LmtPrice']

    def __init__(self, orderInfo_list:list, logfile_path:str='./', logfile_name:str='placeOrder', log_mode:str="save_and_print"):
        """
        Constructor for the App class.
        
        Parameters:
        - orderInfo_list (list): List of order information dictionaries.
        - logfile_path (str): Path to the log file.
        - logfile_name (str): Name of the log file.
        - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
        """
        EClient.__init__(self, self)
        self.logfile_path = logfile_path
        self.logfile_name = logfile_name
        self.logger = log.create_logger(logfile_path, logfile_name, log_mode)
        self.nextorderId = None
        self.orderInfo_list = orderInfo_list
        self.record = []

    def record_placed_order(self, orderId: int, contract: Contract, order: Order):
        """
        Records the details of a placed order.
        
        Parameters:
        - orderId (int): The ID of the order.
        - contract (Contract): Contract object associated with the order.
        - order (Order): Order object containing order details.
        
        Returns:
        - None: This method does not return anything.
        """
        content = [
            self.clientId,
            orderId,
            order.account,
            contract.symbol,
            contract.secType,
            contract.currency,
            contract.exchange,
            contract.primaryExchange,
            order.action,
            order.totalQuantity,
            order.orderType,
            order.tif,
            order.lmtPrice,
        ]
        self.record.append(content)

    def nextValidId(self, orderId: int):
        """
        Callback for receiving the next valid order ID.
        
        Parameters:
        - orderId (int): The next valid order ID.
        
        Returns:
        - None: This method does not return anything.
        """
        super().nextValidId(orderId)
        self.nextorderId = orderId
        self.place_order()

    def error(self, reqId, errorCode, errorString):
        """
        Callback for handling errors.
        
        Parameters:
        - reqId: Request ID associated with the error.
        - errorCode: Error code received.
        - errorString: Description of the error.
        
        Returns:
        - None: This method does not return anything.
        """
        super().error(reqId, errorCode, errorString)

    def place_order(self):
        """
        Places the orders based on the provided order information.
        
        Returns:
        - None: This method does not return anything.
        """
        for orderInfo in self.orderInfo_list:
            contract, order = order_objectizing(orderInfo)
            self.placeOrder(self.nextorderId, contract, order)
            self.record_placed_order(self.nextorderId, contract, order)
            self.nextorderId += 1

        # Disconnect after 3 seconds
        timer = threading.Timer(3, self.stop)
        timer.start()

    def stop(self):
        """
        Disconnects from the TWS API and closes the logger.
        
        Returns:
        - None: This method does not return anything.
        """
        self.disconnect()
        time.sleep(0.5)  # Allowing time for disconnection messages to be processed
        log.close_logger(self.logger)

    def get_placed_orders(self):
        """
        Retrieves the list of placed orders as a DataFrame.
        
        Returns:
        - pd.DataFrame: DataFrame containing placed order details.
        """
        return pd.DataFrame(self.record, columns=self.record_header)

def order_objectizing(order:dict):
    """
    Transforms a dictionary containing order details into Contract and Order objects.
    
    Parameters:
    - order (dict): Dictionary containing order details.
    
    Returns:
    - tuple: A tuple containing a Contract and an Order object.
    """
    EXCHANGE = 'SMART'
    PRIMARYEXCHANGE = 'ARCA'

    contract_obj = Contract()
    contract_obj.symbol = order['Symbol']
    contract_obj.secType = order['SecType']
    contract_obj.currency = 'USD'
    contract_obj.exchange = EXCHANGE
    contract_obj.primaryExchange = PRIMARYEXCHANGE

    order_obj = Order()
    order_obj.account = order['AccountName']
    order_obj.action = order['Action']
    order_obj.totalQuantity = order['TotalQuantity']
    order_obj.orderType = order['OrderType']
    order_obj.tif = order['Tif']
    order_obj.openClose = 'C' if order_obj.action == 'SELL' else 'O'
    order_obj.lmtPrice = order['LmtPrice']
    order_obj.eTradeOnly = ''
    order_obj.firmQuoteOnly = ''

    return contract_obj, order_obj
