import MetaTrader5 as mt5

def connect_mt5(account, password, server, path=None):
    """
    Connects to MetaTrader 5 using the provided credentials.
    path: path to terminal64.exe if needed.
    """
    if path:
        initialized = mt5.initialize(path=path)
    else:
        initialized = mt5.initialize()
        
    if not initialized:
        print("initialize() failed, error code =", mt5.last_error())
        return False

    # connect to the trade account specifying the password and server
    authorized = mt5.login(account, password=password, server=server)
    if authorized:
        print(f"Connected to account #{account}")
        return True
    else:
        print(f"Failed to connect at account #{account}, error code: {mt5.last_error()}")
        return False

if __name__ == "__main__":
    # --- Configuration ---
    ACCOUNT = 12345678 # Replace with your account number
    PASSWORD = "your_password" # Replace with your password
    SERVER = "your_broker_server" # Replace with your broker's server name e.g., "MetaQuotes-Demo"
    
    # 1. Connect
    if connect_mt5(ACCOUNT, PASSWORD, SERVER):
        print("Successfully logged into MT5.")
        mt5.shutdown()
