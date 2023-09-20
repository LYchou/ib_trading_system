import time
from datetime import datetime

def parse_execution_time(execution_time:str) -> datetime:
    '''
    Parses the execution time in the format "HH:MM" (24-hour clock).

    Args:
        execution_time: The time of execution in the format "HH:MM" (24-hour clock).

    Returns:
        datetime: A datetime object representing the specified execution time.
    '''
    # Get the current date and time
    current_time = datetime.now()
    # Parse the execution time
    execution_hour, execution_minute = map(int, execution_time.split(":"))
    # Set the specified time for today
    execution_time = current_time.replace(hour=execution_hour, minute=execution_minute, second=0, microsecond=0)

    return execution_time

def seconds_until_execution(execution_time:str) -> float:
    """
    Calculate the number of seconds remaining until the specified execution time.

    Args:
        execution_time: The time of execution in the format "HH:MM" (24-hour clock).

    Returns:
        float: The number of seconds remaining until the specified execution time.
    """
    current_time = datetime.now()
    execution_time = parse_execution_time(execution_time)
    time_difference = (execution_time - current_time).total_seconds()
    return time_difference

def is_time_expired(execution_time:str, machine_computation_time_protect:int=60) -> None:
    """
    Check whether the current time has already exceeded the specified time.

    Args:
        execution_time: The time of execution in the format "HH:MM" (24-hour clock).
        machine_computation_time_protect: The number of seconds for machine computation time protection.

    Returns:
        None
    """

    wait_seconds = seconds_until_execution(execution_time)
    wait_seconds -= machine_computation_time_protect
    current_time = datetime.now()

    # Raise an AssertionError as a warning if the current time has already exceeded the specified time
    assert wait_seconds > 0, f"Error: The current time ({current_time}) plus machine computation protection time of {machine_computation_time_protect} seconds has already exceeded {execution_time}."

def execute_func_at_time(func, func_args:dict, execution_time:str, machine_computation_time_protect:int=60):
    """
    Execute the given function at the specified time.

    Args:
        func: The function to be executed.
        func_args: The arguments to be passed to the function as a dictionary.
        execution_time: The time of execution in the format "HH:MM" (24-hour clock).
        machine_computation_time_protect: The number of seconds for machine computation time protection.

    Returns:
        None
    """
    # Check if the specified time has already passed
    if is_time_expired(execution_time, machine_computation_time_protect):
        return

    # Calculate the number of seconds remaining until the specified time
    wait_seconds = seconds_until_execution(execution_time)
    # Wait until the specified time
    time.sleep(wait_seconds)

    # When the specified time is reached, execute the func() function and pass func_args as parameters
    func(**func_args)

if __name__ == "__main__":

    # Example function for testing
    def example_func(name, age):
        print(f"Hello, {name}! You are {age} years old.")

    # Set the execution time and parameters
    execution_time = "14:46"
    func_arguments = {"name": "Alice", "age": 30}

    # Execute the function
    execute_func_at_time(example_func, func_arguments, execution_time, timezome='')
