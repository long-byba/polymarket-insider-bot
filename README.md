
# Polymarket Insider Bot

A Telegram bot that watches Polymarket trades and flags suspicious wallet behavior.

The idea is simple:

- new wallet  
- very few bets  
- large position  
- all-in on one outcome  

That kind of behavior is... interesting

## What it does

The bot monitors public Polymarket trades, groups raw fills into logical bets, analyzes wallet history, and sends alerts to Telegram.

## Signal types

### 🔥 INSIDER SIGNAL DETECTED

Strict filter:

- wallet is relatively new  
- very few grouped bets  
- large bet  
- high concentration in one position  

### 🐋 BIG BET / ACTIVE WALLET

- large bet  
- wallet does NOT pass strict insider filters  
- still worth watching  

## Features

- Polymarket Data API integration  
- Gamma API (categories & titles)  
- Telegram alerts  
- SQLite storage  
- grouping of raw fills into real bets  
- heartbeat & logs  
- separate thresholds for:
  - insider signals  
  - big bet alerts  

⚠️p.s: his bot does NOT prove insider trading⚠️
It simply highlights wallet behavior patterns that may look unusual
Sometimes it's very accurate
Sometimes it's just a big degen ^-^

⚠️p.s_2: if it doesn't work -  don't worry :-)
