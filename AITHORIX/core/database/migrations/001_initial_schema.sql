-- AITHORIX Initial Database Schema
-- PostgreSQL 15+ with TimescaleDB

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "timescaledb";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create custom types
CREATE TYPE order_side AS ENUM ('BUY', 'SELL');
CREATE TYPE order_type AS ENUM ('MARKET', 'LIMIT', 'STOP_LOSS', 'TAKE_PROFIT', 'TRAILING_STOP');
CREATE TYPE order_status AS ENUM ('PENDING', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED', 'EXPIRED');

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id VARCHAR(64) UNIQUE NOT NULL,
    exchange_order_id VARCHAR(128),
    symbol VARCHAR(32) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    side order_side NOT NULL,
    order_type order_type NOT NULL,
    status order_status NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    price DECIMAL(20, 8),
    stop_price DECIMAL(20, 8),
    filled_quantity DECIMAL(20, 8) DEFAULT 0,
    average_price DECIMAL(20, 8),
    leverage INTEGER DEFAULT 1,
    reduce_only BOOLEAN DEFAULT FALSE,
    post_only BOOLEAN DEFAULT FALSE,
    time_in_force VARCHAR(16) DEFAULT 'GTC',
    commission DECIMAL(20, 8) DEFAULT 0,
    commission_asset VARCHAR(16),
    signal_id UUID REFERENCES signals(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_order_id ON orders(order_id);
CREATE INDEX idx_orders_symbol_status ON orders(symbol, status);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_orders_exchange ON orders(exchange);

-- Positions table
CREATE TABLE IF NOT EXISTS positions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    position_id VARCHAR(64) UNIQUE NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    side order_side NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    entry_price DECIMAL(20, 8) NOT NULL,
    mark_price DECIMAL(20, 8) NOT NULL,
    liquidation_price DECIMAL(20, 8),
    unrealized_pnl DECIMAL(20, 8) DEFAULT 0,
    realized_pnl DECIMAL(20, 8) DEFAULT 0,
    leverage INTEGER DEFAULT 1,
    margin_type VARCHAR(16) DEFAULT 'CROSS',
    is_active BOOLEAN DEFAULT TRUE,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_positions_symbol_active ON positions(symbol, is_active);
CREATE UNIQUE INDEX uq_active_position_per_symbol_exchange ON positions(symbol, exchange, is_active) WHERE is_active = TRUE;

-- Signals table
CREATE TABLE IF NOT EXISTS signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id VARCHAR(64) NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    side order_side NOT NULL,
    confidence DECIMAL(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    predicted_price DECIMAL(20, 8) NOT NULL,
    predicted_timeframe INTEGER NOT NULL,
    risk_score DECIMAL(5, 4) NOT NULL CHECK (risk_score >= 0 AND risk_score <= 1),
    features JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_signals_created_at ON signals(created_at DESC);
CREATE INDEX idx_signals_model_symbol ON signals(model_id, symbol);

-- Trades table
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trade_id VARCHAR(128) UNIQUE NOT NULL,
    order_id UUID NOT NULL REFERENCES orders(id),
    symbol VARCHAR(32) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    side order_side NOT NULL,
    quantity DECIMAL(20, 8) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    commission DECIMAL(20, 8) DEFAULT 0,
    commission_asset VARCHAR(16),
    is_maker BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_trades_created_at ON trades(created_at DESC);
CREATE INDEX idx_trades_symbol ON trades(symbol);
CREATE INDEX idx_trades_order_id ON trades(order_id);

-- Market data table (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS market_data (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    exchange VARCHAR(32) NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    bid_price DECIMAL(20, 8),
    ask_price DECIMAL(20, 8),
    bid_volume DECIMAL(20, 8),
    ask_volume DECIMAL(20, 8)
);

SELECT create_hypertable('market_data', 'time');
CREATE INDEX idx_market_data_symbol_time ON market_data(symbol, time DESC);

-- Model performance table
CREATE TABLE IF NOT EXISTS model_performance (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    model_id VARCHAR(64) NOT NULL,
    date DATE NOT NULL,
    predictions_count INTEGER DEFAULT 0,
    correct_predictions INTEGER DEFAULT 0,
    accuracy DECIMAL(5, 4),
    total_pnl DECIMAL(20, 8) DEFAULT 0,
    win_rate DECIMAL(5, 4),
    sharpe_ratio DECIMAL(10, 4),
    avg_confidence DECIMAL(5, 4),
    avg_risk_score DECIMAL(5, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_model_performance_model_date ON model_performance(model_id, date);
CREATE UNIQUE INDEX uq_model_performance_per_day ON model_performance(model_id, date);

-- Account balances table
CREATE TABLE IF NOT EXISTS account_balances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    exchange VARCHAR(32) NOT NULL,
    asset VARCHAR(16) NOT NULL,
    free DECIMAL(20, 8) NOT NULL,
    locked DECIMAL(20, 8) NOT NULL,
    total DECIMAL(20, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_balance_per_exchange_asset ON account_balances(exchange, asset);

-- Risk metrics table (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS risk_metrics (
    time TIMESTAMPTZ NOT NULL,
    portfolio_value DECIMAL(20, 8) NOT NULL,
    var_95 DECIMAL(20, 8),
    cvar_95 DECIMAL(20, 8),
    max_drawdown DECIMAL(10, 4),
    sharpe_ratio DECIMAL(10, 4),
    position_count INTEGER DEFAULT 0,
    total_exposure DECIMAL(20, 8),
    leverage_ratio DECIMAL(10, 4),
    correlation_risk DECIMAL(5, 4),
    concentration_risk DECIMAL(5, 4)
);

SELECT create_hypertable('risk_metrics', 'time');

-- System logs table
CREATE TABLE IF NOT EXISTS system_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMPTZ NOT NULL,
    level VARCHAR(16) NOT NULL,
    module VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    details JSONB
);

CREATE INDEX idx_system_logs_timestamp_level ON system_logs(timestamp DESC, level);
CREATE INDEX idx_system_logs_module ON system_logs(module);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to all tables with updated_at
CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_positions_updated_at BEFORE UPDATE ON positions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_signals_updated_at BEFORE UPDATE ON signals FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_trades_updated_at BEFORE UPDATE ON trades FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_model_performance_updated_at BEFORE UPDATE ON model_performance FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_account_balances_updated_at BEFORE UPDATE ON account_balances FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Performance optimization
ALTER TABLE orders SET (fillfactor = 90);
ALTER TABLE positions SET (fillfactor = 90);
ALTER TABLE trades SET (fillfactor = 90);

-- Compression policy for TimescaleDB
SELECT add_compression_policy('market_data', INTERVAL '7 days');
SELECT add_compression_policy('risk_metrics', INTERVAL '30 days');

-- Retention policy
SELECT add_retention_policy('market_data', INTERVAL '2 years');
SELECT add_retention_policy('system_logs', INTERVAL '90 days');

-- Continuous aggregates for performance
CREATE MATERIALIZED VIEW market_data_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS hour,
    symbol,
    exchange,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM market_data
GROUP BY hour, symbol, exchange;

SELECT add_continuous_aggregate_policy('market_data_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Grants
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO aithorix;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO aithorix;
