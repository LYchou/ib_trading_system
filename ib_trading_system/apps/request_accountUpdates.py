from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

import time
import threading
import pandas as pd

from . import log

def main(port: int = 7496, clientId: int = 0, logfile_path: str = './', 
         logfile_name: str = 'account_summary', log_mode:str="save_and_print") -> pd.DataFrame:
    """
    Connect to the IB API, retrieve account summary and disconnect.

    Parameters:
    - port: Port to connect to IB API.
    - clientId: Client ID for the connection.
    - logfile_path: Directory for log files.
    - logfile_name: Name for the log file.
    - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".

    Returns:
    - DataFrame: Contains executions and commissions data.
    """
    app = App(logfile_path, logfile_name, log_mode)
    app.connect('127.0.0.1', port, clientId)
    app.run()

    return app.get_updateAccountValue(), app.get_updatePortfolio()

class App(EWrapper, EClient):
    def __init__(self, logfile_path: str = './', logfile_name: str = 'account_summary', log_mode:str="save_and_print"):
        """
        Initialize App.

        Parameters:
        - logfile_path: Directory for log files.
        - logfile_name: Name for the log file.
        - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
        """
        EClient.__init__(self, self)
        self.logfile_path = logfile_path
        self.logfile_name = logfile_name
        self.logger = log.create_logger(logfile_path, logfile_name, log_mode)

        self.accountsList = []
        self._accountName_index = -1

        self.updateAccountValue_list = []
        self.updatePortfolio_list = []

        # Define headers for the data
        self.updateAccountValue_header = ['Account', 'Key', 'Val', 'Currency']
        self.updatePortfolio_header = [
            'Account', 'ConId', 'Symbol', 'SecType', 'LastTradeDateOrContractMonth', 'Striker', 'Right',
            'Currncy', 'Position', 'MarketPrice', 'MarketValue', 'AverageCost', 'UnrealizedPNL', 'realizedPNL']

    def fetches_accountName(self) -> str:
        """
        Fetches account name in order.

        Returns:
        - str: Current account name.
        """
        if self.accountsList and (self._accountName_index+1) < len(self.accountsList):
            self._accountName_index += 1
            return self.accountsList[self._accountName_index]
        return ''

    # Define the functions for the EWrapper callbacks.
    def nextValidId(self, orderId: int):
        """
        Fetch the next valid order ID and request account updates.

        Parameters:
        - orderId: Order ID received from TWS.
        """
        super().nextValidId(orderId)
        accountName = self.fetches_accountName()
        if accountName:
            self.reqAccountUpdates(True, accountName)

    def managedAccounts(self, accountsList: str):
        """
        Callback for managed accounts. Initializes account list and requests IDs.

        Parameters:
        - accountsList: Comma separated list of account names.
        """
        super().managedAccounts(accountsList)
        self.accountsList = [accountName.strip() for accountName in accountsList.split(',') if accountName.strip()]
        self.reqIds(-1)

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        """
        Updates account value list upon receiving update.

        Parameters:
        - key, val, currency, accountName: Account update details.
        """
        super().updateAccountValue(key, val, currency, accountName)
        content = [accountName, key, val, currency]
        self.updateAccountValue_list.append(content)

    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str):
        """
        Updates portfolio list upon receiving update.

        Parameters:
        - contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName: Portfolio update details.
        """
        super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName)
        content = [
            accountName, contract.conId, contract.symbol, contract.secType, 
            contract.lastTradeDateOrContractMonth, contract.strike, contract.right, 
            contract.currency, position, marketPrice, marketValue, 
            averageCost, unrealizedPNL, realizedPNL
        ]
        self.updatePortfolio_list.append(content)

    def accountDownloadEnd(self, accountName: str):
        """
        Stops account data update and checks if it's the last account, then disconnects.

        Parameters:
        - accountName: Account name where download ended.
        """
        super().accountDownloadEnd(accountName)
        self.reqAccountUpdates(False, accountName)
        self.reqIds(-1)
        if accountName == self.accountsList[-1]:
            timer = threading.Timer(3, self.stop)
            timer.start()

    def error(self, reqId, errorCode, errorString):
        """Handles error messages from TWS."""
        super().error(reqId, errorCode, errorString)

    def stop(self):
        """Disconnects from the TWS and closes logger."""
        self.disconnect()
        time.sleep(0.5)
        log.close_logger(self.logger)

    def get_updateAccountValue(self):
        return pd.DataFrame(self.updateAccountValue_list, columns=self.updateAccountValue_header)

    def get_updatePortfolio(self):
        return pd.DataFrame(self.updatePortfolio_list, columns=self.updatePortfolio_header)