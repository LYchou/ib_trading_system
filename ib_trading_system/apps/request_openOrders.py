from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import pandas as pd
import time

from . import log

def main(port:int=7497, clientId:int=0, logfile_path:str='./', logfile_name:str='AllOpenOrders', log_mode:str="save_and_print") -> tuple:
    """
    This main function initializes the IB (Interactive Brokers) connection, retrieves all open orders, and then disconnects.
    
    Args:
        port (int): Port to connect to IB. Default is 7497.
        clientId (int): Client ID for the session. Default is 0.
        logfile_path (str): The directory path where logs are stored.
        logfile_name (str): Name of the logfile.
        log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
    
    Returns:
        tuple: Contains two pandas DataFrames - one for open orders and another for order status.
    """
    
    app = App(logfile_path, logfile_name, log_mode)
    app.connect('127.0.0.1', port, clientId)
    app.run()

    return app.get_openOrder(), app.get_openOrderStatus()


class App(EWrapper, EClient):
    """
    The App class acts as a client for IB (Interactive Brokers) that retrieves all open orders and their statuses.
    """
    
    def __init__(self, logfile_path:str='./', logfile_name:str='AllOpenOrders', log_mode:str="save_and_print"):
        EClient.__init__(self, self)
        
        # Logger setup
        self.logfile_name = logfile_name
        self.logger = log.create_logger(logfile_path, logfile_name, log_mode)
        
        # Lists to store order and order status details
        self.openOrder_record = list()
        self.orderStatus_record = list()

        # Headers for open orders and order status data
        self.header_openOrder = ['PermId', 'ClientId', 'OrderId', 'Status', 'Symbol', 'SecType', 'LastTradeDateOrContractMonth', 'Strike', 'Right', 'Multiplier', 'Action', 'TotalQuantity', 'OrderType', 'LmtPrice', 'Tif']
        self.header_openOrderStatus = ['PermId', 'ClientId', 'OrderId', 'Status', 'Filled', 'Remaining', 'AvgFillPrice', 'LastFillPrice']
    
    def error(self, reqId, errorCode, errorString):
        """
        This method is called when an error occurs.
        """
        super().error(reqId, errorCode, errorString)

    def nextValidId(self, orderId: int):
        """
        This method is called when the API starts to feed next valid order IDs.
        """
        super().nextValidId(orderId)
        # Requesting all open orders
        self.reqAllOpenOrders()

    def openOrder(self, orderId, contract, order, orderState):
        """
        This method is called for every order as it's returned by Interactive Brokers.
        """
        super().openOrder(orderId, contract, order, orderState)
        
        # Extracting order details and adding them to the openOrder_record list
        content = [
            order.permId,
            order.clientId,
            orderId,
            orderState.status,
            contract.symbol,
            contract.secType,
            contract.lastTradeDateOrContractMonth,
            contract.strike,
            contract.right,
            contract.multiplier,
            order.action,
            order.totalQuantity,
            order.orderType,
            order.lmtPrice,
            order.tif
        ]
        self.openOrder_record.append(content)
    
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        """
        This method provides the current status of an order.
        """
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        
        # Extracting order status details and adding them to the orderStatus_record list
        content = [
            permId,
            clientId,
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            lastFillPrice
        ]
        self.orderStatus_record.append(content)

    def openOrderEnd(self):
        """
        This method indicates the end of the initial orders download.
        """
        super().openOrderEnd()
        
        # Disconnecting after a short delay
        timer = threading.Timer(3, self.stop)
        timer.start()  # Starting the timer

    def stop(self):
        """
        Disconnects from the IB API and closes the logger.
        """
        self.disconnect()
        time.sleep(0.5)
        log.close_logger(self.logger)
        
    def get_openOrder(self) -> pd.DataFrame:
        """
        Retrieves all open orders as a pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing details of all open orders.
        """
        df = pd.DataFrame(self.openOrder_record, columns=self.header_openOrder)
        return df

    def get_openOrderStatus(self) -> pd.DataFrame:
        """
        Retrieves the status of all open orders as a pandas DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing status details of all open orders.
        """
        df = pd.DataFrame(self.orderStatus_record, columns=self.header_openOrderStatus)
        return df
