#!/bin/bash

echo "Creating AITHORIX complete directory structure..."

# Core directories
mkdir -p core/{engine,executor,coordinator,api,websocket,auth,database,cache}

# Models structure (175 models)
mkdir -p models/{directional,execution,behavioral,market,risk}
mkdir -p models/directional/{lstm,transformer,cnn,temporal,wavenet,prophet,bidirectional,residual,attention,graph,neural_ode,mixture,seq2seq,dilated,ensemble}
mkdir -p models/execution/{twap,vwap,iceberg,slippage,impact,router,liquidity,fill,pressure,hidden,queue,shortfall}
mkdir -p models/behavioral/{trader,typing,mouse,decision,emotional,learning,fatigue,session,weekend,error,personality,stress,social,habit,attention}
mkdir -p models/market/{sentiment,onchain,microstructure}
mkdir -p models/market/sentiment/{twitter,reddit,news,telegram,youtube,discord,google,fear_greed,volume,influencer,lag,whale,options,funding,aggregator}
mkdir -p models/market/onchain/{whale,exchange,network,clustering,miner,defi,staking,unlock,hashrate,mempool}
mkdir -p models/market/microstructure/{orderbook,tape,maker,wash,spoofing,institutional,liquidity,frontrun,efficiency,momentum}
mkdir -p models/risk/{portfolio,dynamic,realtime}
mkdir -p models/risk/portfolio/{var,shortfall,drawdown,correlation,concentration,liquidity,counterparty,model,tail,stress,operational,regulatory,blackswan,heatmap,budget}
mkdir -p models/risk/dynamic/{delta,cross,volatility,correlation_hedge,tail_hedge,beta,gamma,vega,currency,basis}
mkdir -p models/risk/realtime/{position_limit,leverage,margin,liquidity_crunch,flash,circuit,anomaly,attribution,budget_monitor,emergency}

# Exchanges
mkdir -p exchanges/{binance,hyperliquid,mexc,bybit,okx,common}
mkdir -p exchanges/binance/{spot,futures,options,websocket,auth}
mkdir -p exchanges/hyperliquid/{perpetual,websocket,auth,gas}
mkdir -p exchanges/mexc/{spot,futures,websocket,auth}
mkdir -p exchanges/bybit/{derivatives,spot,websocket,auth}
mkdir -p exchanges/okx/{unified,websocket,auth}

# Stealth system
mkdir -p stealth/{behavior,antidetection,profiles}
mkdir -p stealth/behavior/{simulator,profiles,patterns,randomization}
mkdir -p stealth/antidetection/{fingerprint,api_normalizer,order_humanizer,timing_jitter,session_auth,geographic,bandwidth,connection,drift,device,network,cognitive,social,adaptive}
mkdir -p stealth/profiles/{binance_institutional,hyperliquid_defi,mexc_retail,bybit_professional,okx_asian}

# Infrastructure
mkdir -p infrastructure/{setup,config,scripts,monitoring,security,backup}

# Monitoring
mkdir -p monitoring/{metrics,alerts,dashboards,logs}
mkdir -p monitoring/metrics/{prometheus,grafana,custom}
mkdir -p monitoring/alerts/{telegram,email,sms,webhook}
mkdir -p monitoring/logs/{aggregator,analyzer,archiver}

# Deployment
mkdir -p deployment/{docker,ansible,terraform,kubernetes,scripts}

# Data directories
mkdir -p data/{historical,realtime,processed,models,cache,backtest}
mkdir -p data/models/{configs,weights,metadata}

# Tests
mkdir -p tests/{unit,integration,performance,security,fixtures,mocks}

# Documentation
mkdir -p docs/{api,architecture,operations,development,deployment}

# Configuration
mkdir -p config/{exchanges,models,trading,security,monitoring}

# Scripts
mkdir -p scripts/{setup,maintenance,monitoring,deployment,utils}

# Utils
mkdir -p utils/{crypto,math,data,network,system}

# Logs
mkdir -p logs/{trading,system,audit,debug}

# Create all __init__.py files
find . -type d -name "*.pyc" -prune -o -type d -exec touch {}/__init__.py \; 2>/dev/null

echo "✅ Directory structure created!"
