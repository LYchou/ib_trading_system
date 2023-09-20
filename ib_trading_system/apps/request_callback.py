from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import ExecutionFilter
import time
import threading
import pandas as pd
from . import log

def main(port:int=7496, clientId:int=0, logfile_path:str='./', logfile_name:str='callback', log_mode:str="save_and_print") -> pd.DataFrame:
    """
    Connects to the Interactive Brokers (IB) API, retrieves executions and commission data, then returns it.

    Args:
    - port (int): The port number to connect to the IB gateway.
    - clientId (int): The client ID to be used with the IB gateway.
    - logfile_path (str): Path to where log files are to be stored.
    - logfile_name (str): Name of the log file.
    - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".

    Returns:
    - pd.DataFrame: DataFrames containing the execution and commission data.
    """
    app = App(logfile_path, logfile_name, log_mode)
    app.connect('127.0.0.1', port, clientId)
    app.run()
    return app.get_executions(), app.get_commissions()

class App(EWrapper, EClient):
    """
    Main application to interact with the Interactive Brokers (IB) API.

    Inheriting from `EWrapper` and `EClient` which are the base classes provided by the IB API.
    """
    def __init__(self, logfile_path:str='./', logfile_name:str='callback', log_mode:str="save_and_print"):
        EClient.__init__(self, self)
        self.logfile_path = logfile_path
        self.logfile_name = logfile_name
        self.logger = log.create_logger(logfile_path, logfile_name, log_mode)

        self.order_id = None
        self.exec_list = []
        self.commission_list = []

        # Headers for the DataFrame representation of execution and commission data.
        self.exec_header = ['PermId', 'ExecId', 'ClientId', 'OrderId', 'Account', 'Symbol', 'SecType', 'Side', 'Shares', 'Price', 'Time']
        self.commission_header = ['ExecId', 'Commission', 'Currency', 'RealizedPNL', 'Yield', '	YieldRedemptionDate']

    def nextValidId(self, orderId:int):
        """
        Callback when the API returns the next valid order ID.

        Args:
        - orderId (int): The next valid order ID.
        """
        super().nextValidId(orderId)
        self.order_id = orderId
        self.reqExecutions(orderId, ExecutionFilter())

    def error(self, reqId, errorCode, errorString):
        """
        Callback for handling errors.

        Args:
        - reqId: The request ID associated with the error.
        - errorCode: The error code.
        - errorString: The error message string.
        """
        super().error(reqId, errorCode, errorString)

    def execDetails(self, reqId, contract, execution):
        """
        Callback when the API returns details of an execution.

        Args:
        - reqId: The request ID for which execution details are returned.
        - contract: The contract for which execution details are returned.
        - execution: Execution details.
        """
        super().execDetails(reqId, contract, execution)
        content = [
            execution.permId, 
            execution.execId, 
            execution.clientId, 
            execution.orderId, 
            execution.acctNumber, 
            contract.symbol, 
            contract.secType, 
            execution.side, 
            execution.shares, 
            execution.price, 
            execution.time
        ]
        self.exec_list.append(content)

    def commissionReport(self, commissionReport):
        """
        Callback when the API returns commission report.

        Args:
        - commissionReport: The commission report.
        """
        content = [
            commissionReport.execId, 
            commissionReport.commission, 
            commissionReport.currency, 
            commissionReport.realizedPNL, 
            commissionReport.yield_, 
            commissionReport.yieldRedemptionDate
        ]
        self.commission_list.append(content)

    def execDetailsEnd(self, reqId:int):
        """
        Callback indicating the end of execution details.

        Args:
        - reqId (int): The request ID associated with the execution details.
        """
        super().execDetailsEnd(reqId)
        timer = threading.Timer(3, self.stop)
        timer.start()  # Start the timer

    def stop(self):
        """
        Disconnect from the API and clean up resources.
        """
        self.disconnect()
        time.sleep(0.5)  # To let the disconnection message be received
        log.close_logger(self.logger)

    def get_executions(self) -> pd.DataFrame:
        """
        Get executions as a pandas DataFrame.

        Returns:
        - pd.DataFrame: A DataFrame containing execution data.
        """
        df = pd.DataFrame(self.exec_list, columns=self.exec_header)
        return df

    def get_commissions(self) -> pd.DataFrame:
        """
        Get commissions as a pandas DataFrame.

        Returns:
        - pd.DataFrame: A DataFrame containing commission data.
        """
        df = pd.DataFrame(self.commission_list, columns=self.commission_header)
        return df
