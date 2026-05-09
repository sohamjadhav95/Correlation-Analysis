import MetaTrader5 as mt5

def place_trade(symbol, order_type, volume, price=None, sl=None, tp=None, deviation=20, magic=123456):
    """
    Place a trade on MT5.
    order_type: mt5.ORDER_TYPE_BUY or mt5.ORDER_TYPE_SELL
    pass price for limit/stop orders, or leave None for market execution.
    """
    symbol_info = mt5.symbol_info(symbol)
    if not symbol_info:
        print(f"{symbol} not found.")
        return None
        
    if not symbol_info.visible:
        if not mt5.symbol_select(symbol, True):
            print(f"symbol_select({symbol}) failed")
            return None

    # If price is None, assume market execution
    if price is None:
        if order_type == mt5.ORDER_TYPE_BUY:
            price = mt5.symbol_info_tick(symbol).ask
        elif order_type == mt5.ORDER_TYPE_SELL:
            price = mt5.symbol_info_tick(symbol).bid
        else:
            print("Invalid order type for market execution.")
            return None

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type,
        "price": price,
        "sl": sl if sl else 0.0,
        "tp": tp if tp else 0.0,
        "deviation": deviation,
        "magic": magic,
        "comment": "python script order",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC, # Use FOK or appropriate depending on broker
    }
    
    # Check if order is valid before sending
    check_result = mt5.order_check(request)
    if check_result is None or check_result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order check failed, error: {mt5.last_error()}")
        if check_result:
             print(f"Check result: {check_result}")
        return None

    # Send order
    result = mt5.order_send(request)
    
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Order failed, retcode={result.retcode}")
        return None
    
    print(f"Order successfully placed! Ticket: {result.order}")
    return result

def close_position(ticket):
    """
    Close an open position using its ticket number
    """
    position = mt5.positions_get(ticket=ticket)
    if position is None or len(position) == 0:
        print(f"Position {ticket} not found")
        return None
        
    position = position[0]
    symbol = position.symbol
    volume = position.volume
    order_type = position.type
    
    # Determine the opposite order type
    close_type = mt5.ORDER_TYPE_SELL if order_type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    close_price = mt5.symbol_info_tick(symbol).bid if close_type == mt5.ORDER_TYPE_SELL else mt5.symbol_info_tick(symbol).ask
    
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": close_type,
        "position": ticket,
        "price": close_price,
        "deviation": 20,
        "magic": position.magic,
        "comment": "python close order",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"Close failed, retcode={result.retcode}")
        return None
        
    print(f"Position {ticket} closed successfully!")
    return result

if __name__ == "__main__":
    from connect_mt5 import connect_mt5
    
    # --- Configuration ---
    ACCOUNT = 12345678 # Replace with your account number
    PASSWORD = "your_password" # Replace with your password
    SERVER = "your_broker_server" # Replace with your broker's server name e.g., "MetaQuotes-Demo"
    SYMBOL = "EURUSD"
    
    # Connect using the separate module
    if connect_mt5(ACCOUNT, PASSWORD, SERVER):
        # Example of placing a trade (Use cautiously, ideally on a DEMO account)
        # print(f"\\nPlacing a buy order on {SYMBOL}...")
        # trade_result = place_trade(SYMBOL, mt5.ORDER_TYPE_BUY, volume=0.01)
        
        # Example of closing the trade we just opened
        # if trade_result:
            # print(f"Closing position {trade_result.order}")
            # close_position(trade_result.order)
            
        mt5.shutdown()
