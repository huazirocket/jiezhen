import time  # 导入time模块，用于处理时间相关功能，如延时
import json  # 导入json模块，用于处理JSON格式数据
import logging  # 导入logging模块，用于记录日志
import requests  # 导入requests模块，用于发送HTTP请求
from concurrent.futures import ThreadPoolExecutor, as_completed  # 导入线程池执行器，用于并发处理任务
from logging.handlers import TimedRotatingFileHandler  # 导入定时轮转日志处理器，用于日志文件的自动轮转
import okx.Trade as TradeAPI  # 导入OKX交易API
import okx.PublicData as PublicAPI  # 导入OKX公共API
import okx.MarketData as MarketAPI  # 导入OKX市场API
import okx.Account as AccountAPI  # 导入OKX账户API
import pandas as pd  # 导入pandas库，用于数据分析和处理

# 读取配置文件
with open('config.json', 'r') as f:  # 打开config.json文件进行读取
    config = json.load(f)  # 将JSON格式的配置文件加载到config变量中

# 提取配置
okx_config = config['okx']  # 获取OKX相关配置
trading_pairs_config = config.get('tradingPairs', {})  # 获取交易对配置，如果不存在则返回空字典
monitor_interval = config.get('monitor_interval', 60)  # 获取监控间隔时间，默认为60秒
feishu_webhook = config.get('feishu_webhook', '')  # 获取飞书webhook地址，用于发送通知
leverage_value = config.get('leverage', 10)  # 获取杠杆倍数，默认为10倍

# 初始化OKX API客户端
trade_api = TradeAPI.TradeAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')  # 初始化交易API
market_api = MarketAPI.MarketAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')  # 初始化市场API
public_api = PublicAPI.PublicAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')  # 初始化公共API
account_api = AccountAPI.AccountAPI(okx_config["apiKey"], okx_config["secret"], okx_config["password"], False, '0')  # 初始化账户API

# 设置日志
log_file = "log/okx.log"  # 定义日志文件路径
logger = logging.getLogger(__name__)  # 获取当前模块的logger
logger.setLevel(logging.INFO)  # 设置日志级别为INFO

# 配置文件日志处理器
file_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7, encoding='utf-8')  # 创建定时轮转日志处理器，每天午夜轮转，保留7天的日志
file_handler.suffix = "%Y-%m-%d"  # 设置日志文件后缀格式为年-月-日
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # 创建日志格式化器
file_handler.setFormatter(formatter)  # 为文件处理器设置格式化器
logger.addHandler(file_handler)  # 将文件处理器添加到logger

# 配置控制台日志处理器
console_handler = logging.StreamHandler()  # 创建控制台日志处理器
console_handler.setFormatter(formatter)  # 为控制台处理器设置格式化器
logger.addHandler(console_handler)  # 将控制台处理器添加到logger

# 存储合约信息的字典
instrument_info_dict = {}  # 初始化一个空字典，用于存储合约信息

def fetch_and_store_all_instruments(instType='SWAP'):  # 定义函数，获取并存储所有合约信息，默认类型为永续合约
    try:
        logger.info(f"Fetching all instruments for type: {instType}")  # 记录日志，表示开始获取指定类型的合约信息
        response = public_api.get_instruments(instType=instType)  # 调用API获取合约信息
        if 'data' in response and len(response['data']) > 0:  # 检查响应中是否包含数据
            instrument_info_dict.clear()  # 清空合约信息字典
            for instrument in response['data']:  # 遍历所有合约
                instId = instrument['instId']  # 获取合约ID
                instrument_info_dict[instId] = instrument  # 将合约信息存储到字典中
                logger.info(f"Stored instrument: {instId}")  # 记录日志，表示已存储合约信息
        else:
            raise ValueError("Unexpected response structure or no instrument data available")  # 如果响应结构不符合预期或没有数据，抛出异常
    except Exception as e:
        logger.error(f"Error fetching instruments: {e}")  # 记录错误日志
        raise  # 重新抛出异常

def send_feishu_notification(message):  # 定义函数，发送飞书通知
    if feishu_webhook:  # 如果配置了飞书webhook
        headers = {'Content-Type': 'application/json'}  # 设置请求头
        data = {"msg_type": "text", "content": {"text": message}}  # 设置请求数据
        response = requests.post(feishu_webhook, headers=headers, json=data)  # 发送POST请求
        if response.status_code == 200:  # 如果响应状态码为200
            logger.info("飞书通知发送成功")  # 记录成功日志
        else:
            logger.error(f"飞书通知发送失败: {response.text}")  # 记录失败日志

def get_mark_price(instId):  # 定义函数，获取标记价格
    response = market_api.get_ticker(instId)  # 调用API获取行情数据
    if 'data' in response and len(response['data']) > 0:  # 检查响应中是否包含数据
        last_price = response['data'][0]['last']  # 获取最新价格
        return float(last_price)  # 返回浮点数格式的价格
    else:
        raise ValueError("Unexpected response structure or missing 'last' key")  # 如果响应结构不符合预期，抛出异常

def round_price_to_tick(price, tick_size):  # 定义函数，将价格四舍五入到最接近的tick_size的整数倍
    # 计算 tick_size 的小数位数
    tick_decimals = len(f"{tick_size:.10f}".rstrip('0').split('.')[1]) if '.' in f"{tick_size:.10f}" else 0  # 计算tick_size的小数位数

    # 调整价格为 tick_size 的整数倍
    adjusted_price = round(price / tick_size) * tick_size  # 将价格调整为tick_size的整数倍
    return f"{adjusted_price:.{tick_decimals}f}"  # 返回格式化后的价格字符串

def get_historical_klines(instId, bar='1m', limit=241):  # 定义函数，获取历史K线数据，默认为1分钟K线，限制241条
    response = market_api.get_candlesticks(instId, bar=bar, limit=limit)  # 调用API获取K线数据
    if 'data' in response and len(response['data']) > 0:  # 检查响应中是否包含数据
        return response['data']  # 返回K线数据
    else:
        raise ValueError("Unexpected response structure or missing candlestick data")  # 如果响应结构不符合预期，抛出异常

def calculate_atr(klines, period=60):  # 定义函数，计算平均真实范围(ATR)，默认周期为60
    trs = []  # 初始化真实范围列表
    for i in range(1, len(klines)):  # 遍历K线数据
        high = float(klines[i][2])  # 获取当前K线的最高价
        low = float(klines[i][3])  # 获取当前K线的最低价
        prev_close = float(klines[i-1][4])  # 获取前一K线的收盘价
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))  # 计算真实范围
        trs.append(tr)  # 将真实范围添加到列表中
    atr = sum(trs[-period:]) / period  # 计算最近period个周期的平均真实范围
    return atr  # 返回ATR值

def calculate_ema_pandas(data, period):  # 定义函数，使用pandas计算指数移动平均线(EMA)
    """
    使用 pandas 计算 EMA
    :param 收盘价列表
    :param period: EMA 周期
    :return: EMA 值
    """
    df = pd.Series(data)  # 将数据转换为pandas Series
    ema = df.ewm(span=period, adjust=False).mean()  # 计算EMA
    return ema.iloc[-1]  # 返回最后一个EMA值

def calculate_average_amplitude(klines, period=60):  # 定义函数，计算平均振幅，默认周期为60
    amplitudes = []  # 初始化振幅列表
    for i in range(len(klines) - period, len(klines)):  # 遍历最近period个K线
        high = float(klines[i][2])  # 获取最高价
        low = float(klines[i][3])  # 获取最低价
        close = float(klines[i][4])  # 获取收盘价
        amplitude = ((high - low) / close) * 100  # 计算振幅百分比
        amplitudes.append(amplitude)  # 将振幅添加到列表中
    average_amplitude = sum(amplitudes) / len(amplitudes)  # 计算平均振幅
    return average_amplitude  # 返回平均振幅

def cancel_all_orders(instId):  # 定义函数，取消所有挂单
    open_orders = trade_api.get_order_list(instId=instId, state='live')  # 获取当前活跃订单
    order_ids = [order['ordId'] for order in open_orders['data']]  # 提取所有订单ID
    for ord_id in order_ids:  # 遍历所有订单ID
        trade_api.cancel_order(instId=instId, ordId=ord_id)  # 取消订单
    logger.info(f"{instId}挂单取消成功.")  # 记录成功日志

def set_leverage(instId, leverage, mgnMode='isolated', posSide=None):  # 定义函数，设置杠杆倍数
    try:
        body = {  # 构建请求体
            "instId": instId,  # 合约ID
            "lever": str(leverage),  # 杠杆倍数
            "mgnMode": mgnMode  # 保证金模式，默认为逐仓
        }
        if mgnMode == 'isolated' and posSide:  # 如果是逐仓模式且指定了持仓方向
            body["posSide"] = posSide  # 添加持仓方向到请求体
        response = account_api.set_leverage(**body)  # 调用API设置杠杆
        if response['code'] == '0':  # 如果响应码为0，表示成功
            logger.info(f"Leverage set to {leverage}x for {instId} with mgnMode: {mgnMode}")  # 记录成功日志
        else:
            logger.error(f"Failed to set leverage: {response['msg']}")  # 记录失败日志
    except Exception as e:
        logger.error(f"Error setting leverage: {e}")  # 记录错误日志

def place_order(instId, price, amount_usdt, side):  # 定义函数，下单
    if instId not in instrument_info_dict:  # 如果合约信息字典中没有该合约
        logger.error(f"Instrument {instId} not found in instrument info dictionary")  # 记录错误日志
        return  # 返回
    tick_size = float(instrument_info_dict[instId]['tickSz'])  # 获取价格精度
    adjusted_price = round_price_to_tick(price, tick_size)  # 调整价格到合适的精度

    # 将USDT金额转换为合约张数
    response = public_api.get_convert_contract_coin(type='1', instId=instId, sz=str(amount_usdt), px=str(adjusted_price), unit='usds')  # 调用API转换
    if response['code'] == '0':  # 如果响应码为0，表示成功
        sz = response['data'][0]['sz']  # 获取转换后的合约张数
        if float(sz) > 0:  # 如果张数大于0

            pos_side = 'long' if side == 'buy' else 'short'  # 根据买卖方向确定持仓方向
            set_leverage(instId, leverage_value, mgnMode='isolated', posSide=pos_side)  # 设置杠杆
            order_result = trade_api.place_order(  # 下单
                instId=instId,  # 合约ID
                tdMode='isolated',  # 交易模式为逐仓
                posSide=pos_side,  # 持仓方向
                side=side,  # 买卖方向
                ordType='limit',  # 订单类型为限价单
                sz=sz,  # 合约张数
                px=str(adjusted_price)  # 价格
            )
            logger.info(f"Order placed: {order_result}")  # 记录下单结果
        else:
            logger.info(f"{instId}计算出的合约张数太小，无法下单。")  # 记录张数太小的信息
    else:
        logger.info(f"{instId}转换失败: {response['msg']}")  # 记录转换失败的信息
        send_feishu_notification(f"{instId}转换失败: {response['msg']}")  # 发送飞书通知

def process_pair(instId, pair_config):  # 定义函数，处理单个交易对，参数为合约ID和该交易对的配置
    try:  # 开始异常处理块
        mark_price = get_mark_price(instId)  # 获取指定合约的标记价格
        klines = get_historical_klines(instId)  # 获取指定合约的历史K线数据

        # 提取收盘价数据用于计算 EMA
        # K线中的收盘价，顺序要新的在最后 (pandas Series会自动处理)
        close_prices_list = [float(kline[4]) for kline in klines[::-1]]  # 从K线数据中提取收盘价，并反转顺序（新的在前），转换为浮点数列表
        if not close_prices_list:  # 如果收盘价列表为空
            logger.warning(f"{instId} no close prices available.")  # 记录警告日志，表示没有可用的收盘价数据
            return  # 结束当前函数执行
        
        close_prices_series = pd.Series(close_prices_list)  # 将收盘价列表转换为pandas Series对象
        current_price = close_prices_series.iloc[-1] # 获取最新的收盘价（即当前价格）

        # 初始化趋势判断标志
        is_bullish_trend = False  # 初始化多头趋势标志为假
        is_bearish_trend = False  # 初始化空头趋势标志为假

        # 从配置中获取趋势增强参数
        min_ema_separation_pct = pair_config.get('min_ema_separation_pct', 0.001) # 从交易对配置中获取EMA最小分离百分比，默认为0.001 (0.1%)
        trend_confirmation_candles = pair_config.get('trend_confirmation_candles', 1) # 从交易对配置中获取趋势确认所需的K线数量，默认为1

        # EMA 相关配置
        ema_short_period = pair_config.get('ema_short_period')  # 从交易对配置中获取短期EMA周期
        ema_long_period = pair_config.get('ema_long_period')  # 从交易对配置中获取长期EMA周期

        if ema_long_period == 0:  # 如果长期EMA周期配置为0 (特殊标记，表示不区分方向)
            is_bullish_trend = True  # 设置为多头趋势
            is_bearish_trend = True  # 设置为空头趋势
            logger.info(f"{instId} ema_long_period is 0, allowing both long and short orders.")  # 记录日志，表明允许双向挂单
        else:  # 如果长期EMA周期不为0，则进行趋势判断
            # 使用双EMA进行趋势判断
            if ema_short_period is None or ema_long_period is None: # 检查短期或长期EMA周期是否未配置
                logger.warning(f"{instId} ema_short_period or ema_long_period is not configured. No trend identified.") # 记录警告，双EMA周期未配置
            elif ema_short_period >= ema_long_period:  # 如果短期EMA周期大于或等于长期EMA周期 (配置错误)
                logger.warning(f"{instId} ema_short_period ({ema_short_period}) should be less than ema_long_period ({ema_long_period}). No trend identified via dual EMA.")  # 记录警告日志，指出配置错误
            elif len(close_prices_series) < ema_long_period or len(close_prices_series) < trend_confirmation_candles:  # 如果数据长度不足以计算长期EMA或进行趋势确认
                logger.warning(f"{instId} Not enough data for Dual EMA calculation or trend confirmation. Need {max(ema_long_period, trend_confirmation_candles)}, got {len(close_prices_series)}.")  # 记录警告日志，数据不足
            else:  # 数据充足且配置正确，开始计算双EMA
                ema_short_series = close_prices_series.ewm(span=ema_short_period, adjust=False).mean()  # 计算短期EMA序列
                ema_long_series = close_prices_series.ewm(span=ema_long_period, adjust=False).mean()  # 计算长期EMA序列

                # 当前K线的EMA值
                current_ema_short = ema_short_series.iloc[-1]  # 获取最新的短期EMA值
                current_ema_long = ema_long_series.iloc[-1]  # 获取最新的长期EMA值

                # 多头趋势条件
                bullish_current_condition = (current_price > current_ema_short and  # 当前价格大于短期EMA
                                           current_ema_short > current_ema_long and  # 且短期EMA大于长期EMA (金叉状态)
                                           (current_ema_short - current_ema_long) / current_ema_long > min_ema_separation_pct)  # 且短期EMA与长期EMA的分离度大于最小百分比
                
                if bullish_current_condition:  # 如果当前K线满足多头趋势条件
                    bullish_confirmed_historically = True  # 初始化历史确认为真
                    if trend_confirmation_candles > 1:  # 如果需要多于1根K线进行趋势确认
                        for i in range(1, trend_confirmation_candles):  # 遍历之前的 trend_confirmation_candles-1 根K线
                            if len(close_prices_series) <= i or len(ema_short_series) <=i or len(ema_long_series) <=i: # 增加索引检查，防止越界
                                bullish_confirmed_historically = False # 如果数据不足，则历史确认失败
                                break
                            prev_price_val = close_prices_series.iloc[-1-i]  # 获取前第i根K线的收盘价
                            prev_ema_short_val = ema_short_series.iloc[-1-i]  # 获取前第i根K线的短期EMA值
                            prev_ema_long_val = ema_long_series.iloc[-1-i]  # 获取前第i根K线的长期EMA值
                            # 历史K线只检查基本排列和价格位置，不强制检查分离度以避免过于严格
                            if not (prev_price_val > prev_ema_short_val and prev_ema_short_val > prev_ema_long_val):  # 如果历史K线不满足基本多头排列
                                bullish_confirmed_historically = False  # 设置历史确认为假
                                break  # 退出循环
                    if bullish_confirmed_historically:  # 如果历史趋势得到确认
                        is_bullish_trend = True  # 设置为多头趋势

                # 空头趋势条件
                bearish_current_condition = (current_price < current_ema_short and  # 当前价格小于短期EMA
                                           current_ema_short < current_ema_long and  # 且短期EMA小于长期EMA (死叉状态)
                                           (current_ema_long - current_ema_short) / current_ema_long > min_ema_separation_pct) # 且长期EMA与短期EMA的分离度大于最小百分比 (使用较慢的EMA作为分母)

                if bearish_current_condition:  # 如果当前K线满足空头趋势条件
                    bearish_confirmed_historically = True  # 初始化历史确认为真
                    if trend_confirmation_candles > 1:  # 如果需要多于1根K线进行趋势确认
                        for i in range(1, trend_confirmation_candles):  # 遍历之前的 trend_confirmation_candles-1 根K线
                            if len(close_prices_series) <= i or len(ema_short_series) <=i or len(ema_long_series) <=i: # 增加索引检查
                                bearish_confirmed_historically = False
                                break
                            prev_price_val = close_prices_series.iloc[-1-i]  # 获取前第i根K线的收盘价
                            prev_ema_short_val = ema_short_series.iloc[-1-i]  # 获取前第i根K线的短期EMA值
                            prev_ema_long_val = ema_long_series.iloc[-1-i]  # 获取前第i根K线的长期EMA值
                            if not (prev_price_val < prev_ema_short_val and prev_ema_short_val < prev_ema_long_val):  # 如果历史K线不满足基本空头排列
                                bearish_confirmed_historically = False  # 设置历史确认为假
                                break  # 退出循环
                    if bearish_confirmed_historically:  # 如果历史趋势得到确认
                        is_bearish_trend = True  # 设置为空头趋势
                
                logger.info(f"{instId} Dual EMA: Short({ema_short_period}): {current_ema_short:.6f}, Long({ema_long_period}): {current_ema_long:.6f}, Price: {current_price:.6f}. Bullish: {is_bullish_trend}, Bearish: {is_bearish_trend}")  # 记录双EMA的计算结果和趋势判断

        # 计算 ATR
        atr = calculate_atr(klines)  # 计算平均真实波幅(ATR)
        price_atr_ratio = atr / mark_price  # 计算标记价格与ATR的比值
        logger.info(f"{instId} ATR: {atr}, 当前价格/ATR比值: {price_atr_ratio:.3f}")  # 记录ATR和价格ATR比值

        average_amplitude = calculate_average_amplitude(klines)  # 计算平均振幅
        logger.info(f"{instId} ATR: {atr}, 平均振幅: {average_amplitude:.2f}%")  # 记录ATR和平均振幅 (注意这里日志重复记录了ATR，可以考虑调整)

        value_multiplier = pair_config.get('value_multiplier', 2)  # 从交易对配置中获取价值乘数，默认为2
        selected_value = (average_amplitude+price_atr_ratio)/2 * value_multiplier  # 计算选定值，用于确定价格偏移因子 (平均振幅和价格ATR比值的平均值乘以乘数)
        #selected_value = max(selected_value, 0.8)  # 确保值不小于0.8 (此行为注释代码)

        long_price_factor = 1 - selected_value / 100  # 计算多单价格因子 (1 - 选定值百分比)
        short_price_factor = 1 + selected_value / 100  # 计算空单价格因子 (1 + 选定值百分比)

        long_amount_usdt = pair_config.get('long_amount_usdt', 20)  # 从交易对配置中获取多单金额(USDT)，默认为20
        short_amount_usdt = pair_config.get('short_amount_usdt', 20)  # 从交易对配置中获取空单金额(USDT)，默认为20

        target_price_long = mark_price * long_price_factor  # 计算多单目标挂单价格
        target_price_short = mark_price * short_price_factor  # 计算空单目标挂单价格

        logger.info(f"{instId} Long target price: {target_price_long:.6f}, Short target price: {target_price_short:.6f}")  # 记录计算出的多空目标价格

        cancel_all_orders(instId)  # 取消该合约所有当前的挂单

        # 判断趋势后决定是否挂单
        if is_bullish_trend:  # 如果判断为多头趋势
            logger.info(f"{instId} 当前为多头趋势，允许挂多单")  # 记录日志，表明当前为多头趋势，将挂多单
            place_order(instId, target_price_long, long_amount_usdt, 'buy')  # 下多单
        else:  # 如果非多头趋势
            logger.info(f"{instId} 当前非多头趋势，跳过多单挂单")  # 记录日志，表明当前非多头趋势，跳过多单

        if is_bearish_trend:  # 如果判断为空头趋势
            logger.info(f"{instId} 当前为空头趋势，允许挂空单")  # 记录日志，表明当前为空头趋势，将挂空单
            place_order(instId, target_price_short, short_amount_usdt, 'sell')  # 下空单
        else:  # 如果非空头趋势
            logger.info(f"{instId} 当前非空头趋势，跳过空单挂单")  # 记录日志，表明当前非空头趋势，跳过空单

    except Exception as e:  # 捕获处理过程中发生的任何异常
        error_message = f'Error processing {instId}: {e}'  # 构建错误消息
        logger.error(error_message)  # 记录错误日志
        send_feishu_notification(error_message)  # 发送飞书通知告知错误

def main():  # 定义主函数
    fetch_and_store_all_instruments()  # 获取并存储所有合约信息
    inst_ids = list(trading_pairs_config.keys())  # 获取所有币对的ID
    batch_size = 5  # 每批处理的数量

    while True:  # 无限循环
        for i in range(0, len(inst_ids), batch_size):  # 按批次处理币对
            batch = inst_ids[i:i + batch_size]  # 获取当前批次的币对
            with ThreadPoolExecutor(max_workers=batch_size) as executor:  # 创建线程池
                futures = [executor.submit(process_pair, instId, trading_pairs_config[instId]) for instId in batch]  # 提交任务到线程池
                for future in as_completed(futures):  # 等待任务完成
                    future.result()  # 获取任务结果，如果有异常会抛出

        time.sleep(monitor_interval)  # 休眠指定的时间间隔

if __name__ == '__main__':  # 如果是直接运行此脚本
    main()  # 调用主函数