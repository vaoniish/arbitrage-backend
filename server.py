import asyncio
import logging
import os
import time
import traceback
from typing import Dict, List, Optional

import aiohttp
import ccxt.async_support as ccxt
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI(title="Arbitrage Terminal Pro API", version="3.0")

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Переменные окружения для безопасности (берутся из настроек Render)
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
MY_CHAT_ID = os.getenv("MY_CHAT_ID", "")

# Глобальный кэш
CACHE = {
    "deals": [],
    "last_scan_time": 0,
    "scan_duration_sec": 0,
    "sent_alerts": {}  # Память отправленных алертов, чтобы не спамить одной связкой
}

# Конфигурация бирж и комиссий
EXCHANGES_CONFIG = {
    "bybit": {"class": ccxt.bybit, "fee": 0.001, "trade_url": "https://www.bybit.com/trade/usdt/"},
    "mexc": {"class": ccxt.mexc, "fee": 0.001, "trade_url": "https://www.mexc.com/exchange/"},
    "gateio": {"class": ccxt.gateio, "fee": 0.002, "trade_url": "https://www.gate.io/trade/"},
    "bitget": {"class": ccxt.bitget, "fee": 0.001, "trade_url": "https://www.bitget.com/spot/"}
}

TARGET_COINS = ["BTC", "ETH", "SOL", "TON", "XRP", "DOGE", "ADA", "AVAX", "NEAR", "SUI", "LTC", "APT"]

NETWORK_FEES = {
    "BTC": {"fee": 2.5, "net": "Bitcoin Native / BEP20"},
    "ETH": {"fee": 1.5, "net": "Arbitrum / Optimism"},
    "SOL": {"fee": 0.5, "net": "Solana"},
    "TON": {"fee": 0.3, "net": "TON"},
    "XRP": {"fee": 0.25, "net": "Ripple"},
    "DOGE": {"fee": 0.8, "net": "Doge Native"},
    "ADA": {"fee": 0.5, "net": "Cardano"},
    "AVAX": {"fee": 0.4, "net": "Avalanche C-Chain"},
    "NEAR": {"fee": 0.2, "net": "Near Protocol"},
    "SUI": {"fee": 0.1, "net": "Sui Network"},
    "LTC": {"fee": 0.1, "net": "Litecoin Native"},
    "APT": {"fee": 0.15, "net": "Aptos"}
}

async def send_telegram_alert(message: str):
    """Безопасная отправка PUSH-уведомлений в личные сообщения Telegram"""
    if not BOT_TOKEN or not MY_CHAT_ID:
        return
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": MY_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=5) as resp:
                if resp.status != 200:
                    logging.error(f"Ошибка отправки алертов в Telegram: {await resp.text()}")
    except Exception as e:
        logging.error(f"Сбой отправки Telegram алертов: {e}")

def calculate_vwap_buy(orderbook_asks: list, amount_usd: float) -> Optional[float]:
    accumulated_usd = 0.0
    accumulated_coins = 0.0
    for price, volume in orderbook_asks:
        level_usd = price * volume
        if accumulated_usd + level_usd >= amount_usd:
            needed_usd = amount_usd - accumulated_usd
            accumulated_coins += needed_usd / price
            accumulated_usd = amount_usd
            break
        else:
            accumulated_usd += level_usd
            accumulated_coins += volume
    if accumulated_usd < amount_usd or accumulated_coins == 0:
        return None
    return amount_usd / accumulated_coins

def calculate_vwap_sell(orderbook_bids: list, amount_coins: float) -> Optional[float]:
    accumulated_coins = 0.0
    total_received_usd = 0.0
    for price, volume in orderbook_bids:
        if accumulated_coins + volume >= amount_coins:
            needed_coins = amount_coins - accumulated_coins
            total_received_usd += needed_coins * price
            accumulated_coins = amount_coins
            break
        else:
            accumulated_coins += volume
            total_received_usd += volume * price
    if accumulated_coins < amount_coins or amount_coins == 0:
        return None
    return total_received_usd / amount_coins

async def fetch_orderbook(ex_id: str, ex_instance, symbol: str) -> Optional[Dict]:
    try:
        orderbook = await asyncio.wait_for(ex_instance.fetch_order_book(symbol, limit=20), timeout=3.0)
        return {"ex_id": ex_id, "symbol": symbol, "asks": orderbook.get("asks", []), "bids": orderbook.get("bids", [])}
    except Exception:
        return None

async def scan_market_task(user_deposit: float = 500.0) -> List[Dict]:
    start_time = time.time()
    instances = {ex_id: config["class"]({'enableRateLimit': True, 'timeout': 3000}) for ex_id, config in EXCHANGES_CONFIG.items()}

    tasks = []
    for coin in TARGET_COINS:
        symbol = f"{coin}/USDT"
        for ex_id, instance in instances.items():
            tasks.append(fetch_orderbook(ex_id, instance, symbol))

    results = await asyncio.gather(*tasks)

    coin_data = {coin: {} for coin in TARGET_COINS}
    for res in results:
        if res and res["asks"] and res["bids"]:
            coin = res["symbol"].split("/")[0]
            coin_data[coin][res["ex_id"]] = res

    for instance in instances.values():
        await instance.close()

    deals = []
    current_time = time.time()

    for coin, ex_books in coin_data.items():
        if len(ex_books) < 2:
            continue

        net_info = NETWORK_FEES.get(coin, {"fee": 1.0, "net": "Mainnet"})

        for buy_ex, buy_data in ex_books.items():
            for sell_ex, sell_data in ex_books.items():
                if buy_ex == sell_ex:
                    continue

                vwap_buy = calculate_vwap_buy(buy_data["asks"], user_deposit)
                if not vwap_buy:
                    continue

                buy_fee_pct = EXCHANGES_CONFIG[buy_ex]["fee"]
                coins_bought = (user_deposit * (1 - buy_fee_pct)) / vwap_buy

                vwap_sell = calculate_vwap_sell(sell_data["bids"], coins_bought)
                if not vwap_sell:
                    continue

                sell_fee_pct = EXCHANGES_CONFIG[sell_ex]["fee"]
                gross_usd = (coins_bought * vwap_sell) * (1 - sell_fee_pct)

                net_usd = gross_usd - net_info["fee"]
                profit_usd = net_usd - user_deposit
                profit_pct = (profit_usd / user_deposit) * 100

                # Фильтр корректных связок (отсекаем блокировки вывода >15%)
                if -5.0 <= profit_pct <= 15.0:
                    deal_key = f"{coin}_{buy_ex}_{sell_ex}"
                    
                    deal_obj = {
                        "coin": coin,
                        "symbol": f"{coin}/USDT",
                        "buy_ex": buy_ex.upper(),
                        "sell_ex": sell_ex.upper(),
                        "buy_price": round(vwap_buy, 4),
                        "sell_price": round(vwap_sell, 4),
                        "profit_pct": round(profit_pct, 2),
                        "profit_usd": round(profit_usd, 2),
                        "net_fee": net_info["fee"],
                        "network": net_info["net"],
                        "buy_link": f"{EXCHANGES_CONFIG[buy_ex]['trade_url']}{coin}_USDT",
                        "sell_link": f"{EXCHANGES_CONFIG[sell_ex]['trade_url']}{coin}_USDT"
                    }
                    deals.append(deal_obj)

                    # Автоматическая отправка уведомления в Telegram (для сочных связок > 1.5%)
                    if profit_pct >= 1.5:
                        last_sent = CACHE["sent_alerts"].get(deal_key, 0)
                        # Защита от спама: отправляем уведомление по одной паре не чаще чем раз в 5 минут
                        if current_time - last_sent > 300:
                            CACHE["sent_alerts"][deal_key] = current_time
                            msg = (
                                f"🔥 <b>НАЙДЕНА ПРИБЫЛЬНАЯ СВЯЗКА!</b>\n\n"
                                f"🪙 <b>Монета:</b> #{coin} ({net_info['net']})\n"
                                f"📈 <b>Профит:</b> <code>+{round(profit_pct, 2)}%</code> (+${round(profit_usd, 2)})\n\n"
                                f"🛒 <b>Купить:</b> {buy_ex.upper()} по ${round(vwap_buy, 4)}\n"
                                f"💰 <b>Продать:</b> {sell_ex.upper()} по ${round(vwap_sell, 4)}\n\n"
                                f"💸 <b>Комиссия сети:</b> ${net_info['fee']}\n"
                                f"📲 <a href='t.me/biboblllsbot/terminal'>Открыть в Терминале</a>"
                            )
                            asyncio.create_task(send_telegram_alert(msg))

    deals.sort(key=lambda x: x["profit_pct"], reverse=True)

    CACHE["deals"] = deals
    CACHE["last_scan_time"] = int(current_time)
    CACHE["scan_duration_sec"] = round(time.time() - start_time, 2)
    return deals

async def background_scanner_loop():
    while True:
        try:
            await scan_market_task()
        except Exception as e:
            logging.error(f"Ошибка фонового сканера: {e}")
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scanner_loop())

@app.get("/health")
async def health_check():
    """Эндпоинт для авто-пингера (UptimeRobot / Cron-Job) против спячки Render"""
    return {"status": "alive", "timestamp": int(time.time())}

@app.get("/test")
async def test_route():
    """Простой тестовый маршрут для проверки работоспособности сервера"""
    return {"status": "ok", "message": "Бэкенд работает!"}

@app.get("/api/deals")
async def get_deals(deposit: float = Query(500.0, ge=10.0, le=100000.0)):
    try:
        if not CACHE["deals"]:
            await scan_market_task(user_deposit=deposit)

        return {
            "status": "success",
            "scan_time_sec": CACHE["scan_duration_sec"],
            "timestamp": CACHE["last_scan_time"],
            "count": len(CACHE["deals"]),
            "deals": CACHE["deals"]
        }
    except Exception as e:
        error_details = traceback.format_exc()
        logging.error(f"Ошибка в /api/deals: {error_details}")
        return {
            "status": "error",
            "error": str(e),
            "traceback": error_details
        }, 500

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)