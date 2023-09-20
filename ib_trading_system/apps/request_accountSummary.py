from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.account_summary_tags import AccountSummaryTags
import time
import threading
import pandas as pd
from . import log

def main(port:int=7496, clientId:int=0, logfile_path:str='./', logfile_name:str='account_summary', log_mode:str="save_and_print") -> pd.DataFrame:
    """
    Main function to retrieve the account summary from Interactive Brokers API.
    
    Parameters:
    - port (int): The port to connect to. Default is 7496.
    - clientId (int): The client ID for the connection. Default is 0.
    - logfile_path (str): Path to save the logfile. Default is the current directory.
    - logfile_name (str): Name of the logfile. Default is 'account_summary'.
    - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".

    Returns:
    - pd.DataFrame: A dataframe containing the account summary details.
    """

    app = App(logfile_path, logfile_name, log_mode)
    app.connect('127.0.0.1', port, clientId)
    app.run()

    return app.get_accountSummary()

class App(EWrapper, EClient):
    def __init__(self, logfile_path:str='./', logfile_name:str='account_summary', log_mode:str="save_and_print"):
        """
        Initialize the App class.

        Parameters:
        - logfile_path (str): Path to save the logfile. Default is the current directory.
        - logfile_name (str): Name of the logfile. Default is 'account_summary'.
        - log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
        """
        EClient.__init__(self, self)
        self.logfile_path = logfile_path
        self.logfile_name = logfile_name
        self.logger = log.create_logger(logfile_path, logfile_name, log_mode)
        self.order_id = None
        self.accountSummary_list = []
        self.accountSummary_header = ['ReqId', 'Account', 'Tag', 'Value', 'Currency']

    def nextValidId(self, orderId:int):
        """
        Callback for when the next valid order ID is received.

        Parameters:
        - orderId (int): The next valid order ID.
        """
        super().nextValidId(orderId)
        self.order_id = orderId
        self.reqAccountSummary(orderId, "All", AccountSummaryTags.AllTags)

    def error(self, reqId, errorCode, errorString):
        """
        Callback for errors returned by the API.

        Parameters:
        - reqId: ID of the request that caused the error.
        - errorCode: Numeric error code.
        - errorString: String error message.
        """
        super().error(reqId, errorCode, errorString)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        """
        Callback for account summary details received from the API.

        Parameters:
        - reqId (int): Request ID.
        - account (str): Account name.
        - tag (str): Data tag.
        - value (str): Data value.
        - currency (str): Currency of the value.
        """
        super().accountSummary(reqId, account, tag, value, currency)
        content = [reqId, account, tag, value, currency]
        self.accountSummary_list.append(content)

    def accountSummaryEnd(self, reqId: int):
        """
        Callback for when account summary data finishes transmitting.

        Parameters:
        - reqId (int): Request ID.
        """
        super().accountSummaryEnd(reqId)
        timer = threading.Timer(3, self.stop)
        timer.start()  # Start the timer

    def stop(self):
        """Disconnect from the server and close logger."""
        self.disconnect()
        time.sleep(0.5)  # To allow disconnection message to be received
        log.close_logger(self.logger)

    def get_accountSummary(self) -> pd.DataFrame:
        """
        Get the account summary as a dataframe.

        Returns:
        - pd.DataFrame: A dataframe containing the account summary details.
        """
        return pd.DataFrame(self.accountSummary_list, columns=self.accountSummary_header)
