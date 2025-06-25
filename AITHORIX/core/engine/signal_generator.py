"""
AITHORIX Signal Generator
Advanced signal generation from multiple sources

This module handles:
- Signal generation from technical indicators
- ML model signal integration
- Multi-timeframe signal analysis
- Signal filtering and validation
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Set, Tuple, Callable
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from collections import defaultdict, deque
import talib
import uuid

from core.engine.market_data import MarketData, OHLCV, OrderBook, Ticker
from core.coordinator.strategy_coordinator import Signal, SignalType, SignalStrength
from core.engine.order_manager import OrderType, TimeInForce
from utils.helpers import get_timestamp, synchronized
from monitoring.metrics import MetricsCollector


class IndicatorType(Enum):
    """Technical indicator types"""
    TREND = "trend"
    MOMENTUM = "momentum"
    VOLATILITY = "volatility"
    VOLUME = "volume"
    SUPPORT_RESISTANCE = "support_resistance"
    PATTERN = "pattern"


class TimeFrame(Enum):
    """Trading timeframes"""
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"


@dataclass
class TechnicalSignal:
    """Technical indicator signal"""
    indicator: str
    timeframe: TimeFrame
    value: float
    signal_type: str  # "BUY", "SELL", "NEUTRAL"
    strength: float  # 0-1
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatternSignal:
    """Chart pattern signal"""
    pattern_name: str
    timeframe: TimeFrame
    pattern_type: str  # "bullish", "bearish", "neutral"
    confidence: float
    target_price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class CompositeSignal:
    """Aggregated signal from multiple sources"""
    symbol: str
    direction: str  # "LONG", "SHORT", "NEUTRAL"
    strength: SignalStrength
    confidence: float
    entry_price: Decimal
    target_price: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    
    # Component signals
    technical_signals: List[TechnicalSignal] = field(default_factory=list)
    pattern_signals: List[PatternSignal] = field(default_factory=list)
    ml_predictions: Dict[str, Any] = field(default_factory=dict)
    
    # Timeframe analysis
    timeframe_alignment: float = 0.0  # How aligned signals are across timeframes
    
    # Risk metrics
    risk_reward_ratio: Optional[float] = None
    expected_return: float = 0.0
    
    timestamp: datetime = field(default_factory=datetime.utcnow)


class SignalGenerator:
    """
    Advanced signal generation system
    
    Generates trading signals from technical analysis, patterns,
    and ML models with multi-timeframe confirmation.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger("AITHORIX.SignalGenerator")
        
        # Signal configuration
        self.min_confidence = config.get("min_confidence", 0.7)
        self.min_timeframe_alignment = config.get("min_timeframe_alignment", 0.6)
        self.enable_technical = config.get("enable_technical", True)
        self.enable_patterns = config.get("enable_patterns", True)
        self.enable_ml = config.get("enable_ml", True)
        
        # Timeframes to analyze
        self.timeframes = [TimeFrame(tf) for tf in config.get("timeframes", ["5m", "15m", "1h"])]
        
        # Technical indicators configuration
        self.indicators = self._setup_indicators(config.get("indicators", {}))
        
        # Pattern detection configuration
        self.pattern_config = config.get("patterns", {})
        self.min_pattern_confidence = self.pattern_config.get("min_confidence", 0.8)
        
        # Historical data storage
        self.ohlcv_data: Dict[str, Dict[TimeFrame, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=500)))
        
        # Signal history
        self.signal_history: deque = deque(maxlen=1000)
        self.active_signals: Dict[str, CompositeSignal] = {}
        
        # Performance tracking
        self.metrics_collector = MetricsCollector()
        self.signal_performance: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # State
        self._lock = asyncio.Lock()
        self.is_initialized = False
        
    async def initialize(self) -> None:
        """Initialize the signal generator"""
        self.logger.info("Initializing Signal Generator...")
        
        # Start background tasks
        asyncio.create_task(self._signal_monitoring_loop())
        asyncio.create_task(self._performance_tracking_loop())
        
        self.is_initialized = True
        self.logger.info("Signal Generator initialized")
    
    @synchronized
    async def generate_signals(
        self,
        market_data: Dict[str, MarketData],
        ml_predictions: Optional[Dict[str, Any]] = None
    ) -> List[Signal]:
        """Generate trading signals from market data"""
        signals = []
        
        for symbol, data in market_data.items():
            try:
                # Update historical data
                await self._update_historical_data(symbol, data)
                
                # Generate composite signal
                composite_signal = await self._generate_composite_signal(
                    symbol, data, ml_predictions
                )
                
                if composite_signal and self._validate_composite_signal(composite_signal):
                    # Convert to trading signal
                    trading_signal = self._convert_to_trading_signal(composite_signal)
                    if trading_signal:
                        signals.append(trading_signal)
                        
                        # Store active signal
                        self.active_signals[symbol] = composite_signal
                        self.signal_history.append(composite_signal)
                
            except Exception as e:
                self.logger.error(f"Error generating signal for {symbol}: {e}")
        
        return signals
    
    async def _generate_composite_signal(
        self,
        symbol: str,
        market_data: MarketData,
        ml_predictions: Optional[Dict[str, Any]]
    ) -> Optional[CompositeSignal]:
        """Generate composite signal from all sources"""
        # Technical analysis signals
        technical_signals = []
        if self.enable_technical:
            technical_signals = await self._generate_technical_signals(symbol)
        
        # Pattern recognition signals
        pattern_signals = []
        if self.enable_patterns:
            pattern_signals = await self._detect_patterns(symbol)
        
        # ML predictions
        ml_signal = None
        if self.enable_ml and ml_predictions and symbol in ml_predictions:
            ml_signal = ml_predictions[symbol]
        
        # Aggregate signals
        if not technical_signals and not pattern_signals and not ml_signal:
            return None
        
        # Determine overall direction and strength
        direction, strength, confidence = self._aggregate_signals(
            technical_signals, pattern_signals, ml_signal
        )
        
        if direction == "NEUTRAL" or confidence < self.min_confidence:
            return None
        
        # Calculate entry, target, and stop loss
        current_price = self._get_current_price(market_data)
        entry_price, target_price, stop_loss = self._calculate_levels(
            direction, current_price, technical_signals, pattern_signals
        )
        
        # Calculate risk metrics
        risk_reward_ratio = self._calculate_risk_reward(entry_price, target_price, stop_loss)
        expected_return = self._calculate_expected_return(
            entry_price, target_price, confidence
        )
        
        # Calculate timeframe alignment
        timeframe_alignment = self._calculate_timeframe_alignment(technical_signals)
        
        return CompositeSignal(
            symbol=symbol,
            direction=direction,
            strength=strength,
            confidence=confidence,
            entry_price=entry_price,
            target_price=target_price,
            stop_loss=stop_loss,
            technical_signals=technical_signals,
            pattern_signals=pattern_signals,
            ml_predictions=ml_signal or {},
            timeframe_alignment=timeframe_alignment,
            risk_reward_ratio=risk_reward_ratio,
            expected_return=expected_return
        )
    
    async def _generate_technical_signals(self, symbol: str) -> List[TechnicalSignal]:
        """Generate signals from technical indicators"""
        signals = []
        
        for timeframe in self.timeframes:
            # Get OHLCV data for timeframe
            ohlcv_data = self._get_ohlcv_array(symbol, timeframe)
            
            if len(ohlcv_data) < 50:  # Need minimum data
                continue
            
            # Calculate indicators
            for indicator_type, indicators in self.indicators.items():
                for indicator_name, params in indicators.items():
                    signal = self._calculate_indicator_signal(
                        indicator_name, ohlcv_data, timeframe, params
                    )
                    if signal:
                        signals.append(signal)
        
        return signals
    
    def _calculate_indicator_signal(
        self,
        indicator: str,
        ohlcv_data: np.ndarray,
        timeframe: TimeFrame,
        params: Dict[str, Any]
    ) -> Optional[TechnicalSignal]:
        """Calculate signal from a specific indicator"""
        try:
            close_prices = ohlcv_data[:, 3]  # Close prices
            high_prices = ohlcv_data[:, 1]   # High prices
            low_prices = ohlcv_data[:, 2]    # Low prices
            volume = ohlcv_data[:, 4]        # Volume
            
            signal_type = "NEUTRAL"
            strength = 0.0
            value = 0.0
            metadata = {}
            
            # Moving Average signals
            if indicator == "SMA":
                period = params.get("period", 20)
                sma = talib.SMA(close_prices, timeperiod=period)
                
                if len(sma) > 0 and not np.isnan(sma[-1]):
                    value = sma[-1]
                    current_price = close_prices[-1]
                    
                    # Signal based on price position relative to SMA
                    if current_price > sma[-1] * 1.01:  # 1% above
                        signal_type = "BUY"
                        strength = min((current_price - sma[-1]) / sma[-1] * 10, 1.0)
                    elif current_price < sma[-1] * 0.99:  # 1% below
                        signal_type = "SELL"
                        strength = min((sma[-1] - current_price) / sma[-1] * 10, 1.0)
                    
                    metadata["sma_value"] = float(sma[-1])
            
            elif indicator == "EMA":
                period = params.get("period", 12)
                ema = talib.EMA(close_prices, timeperiod=period)
                
                if len(ema) > 0 and not np.isnan(ema[-1]):
                    value = ema[-1]
                    current_price = close_prices[-1]
                    
                    if current_price > ema[-1]:
                        signal_type = "BUY"
                        strength = min((current_price - ema[-1]) / ema[-1] * 15, 1.0)
                    else:
                        signal_type = "SELL"
                        strength = min((ema[-1] - current_price) / ema[-1] * 15, 1.0)
            
            elif indicator == "RSI":
                period = params.get("period", 14)
                rsi = talib.RSI(close_prices, timeperiod=period)
                
                if len(rsi) > 0 and not np.isnan(rsi[-1]):
                    value = rsi[-1]
                    
                    if rsi[-1] < 30:  # Oversold
                        signal_type = "BUY"
                        strength = (30 - rsi[-1]) / 30
                    elif rsi[-1] > 70:  # Overbought
                        signal_type = "SELL"
                        strength = (rsi[-1] - 70) / 30
                    
                    metadata["rsi_value"] = float(rsi[-1])
            
            elif indicator == "MACD":
                fast = params.get("fast", 12)
                slow = params.get("slow", 26)
                signal_period = params.get("signal", 9)
                
                macd, macd_signal, macd_hist = talib.MACD(
                    close_prices, fastperiod=fast, slowperiod=slow, signalperiod=signal_period
                )
                
                if len(macd) > 0 and not np.isnan(macd[-1]):
                    value = macd_hist[-1]
                    
                    # MACD crossover signals
                    if macd_hist[-1] > 0 and macd_hist[-2] <= 0:
                        signal_type = "BUY"
                        strength = 0.8
                    elif macd_hist[-1] < 0 and macd_hist[-2] >= 0:
                        signal_type = "SELL"
                        strength = 0.8
                    
                    metadata["macd"] = float(macd[-1])
                    metadata["signal"] = float(macd_signal[-1])
                    metadata["histogram"] = float(macd_hist[-1])
            
            elif indicator == "BB":
                period = params.get("period", 20)
                std_dev = params.get("std_dev", 2)
                
                upper, middle, lower = talib.BBANDS(
                    close_prices, timeperiod=period, nbdevup=std_dev, nbdevdn=std_dev
                )
                
                if len(upper) > 0 and not np.isnan(upper[-1]):
                    current_price = close_prices[-1]
                    
                    # Position within bands
                    band_width = upper[-1] - lower[-1]
                    position = (current_price - lower[-1]) / band_width if band_width > 0 else 0.5
                    value = position
                    
                    if current_price <= lower[-1]:
                        signal_type = "BUY"
                        strength = 0.7
                    elif current_price >= upper[-1]:
                        signal_type = "SELL"
                        strength = 0.7
                    
                    metadata["upper_band"] = float(upper[-1])
                    metadata["middle_band"] = float(middle[-1])
                    metadata["lower_band"] = float(lower[-1])
            
            elif indicator == "STOCH":
                k_period = params.get("k_period", 14)
                d_period = params.get("d_period", 3)
                
                slowk, slowd = talib.STOCH(
                    high_prices, low_prices, close_prices,
                    fastk_period=k_period, slowk_period=d_period, slowd_period=d_period
                )
                
                if len(slowk) > 0 and not np.isnan(slowk[-1]):
                    value = slowk[-1]
                    
                    if slowk[-1] < 20:
                        signal_type = "BUY"
                        strength = (20 - slowk[-1]) / 20
                    elif slowk[-1] > 80:
                        signal_type = "SELL"
                        strength = (slowk[-1] - 80) / 20
                    
                    metadata["k"] = float(slowk[-1])
                    metadata["d"] = float(slowd[-1])
            
            elif indicator == "ATR":
                period = params.get("period", 14)
                atr = talib.ATR(high_prices, low_prices, close_prices, timeperiod=period)
                
                if len(atr) > 0 and not np.isnan(atr[-1]):
                    value = atr[-1]
                    # ATR is used for volatility, not directional signals
                    metadata["atr"] = float(atr[-1])
                    metadata["atr_percent"] = float(atr[-1] / close_prices[-1] * 100)
            
            if signal_type != "NEUTRAL":
                return TechnicalSignal(
                    indicator=indicator,
                    timeframe=timeframe,
                    value=value,
                    signal_type=signal_type,
                    strength=strength,
                    timestamp=datetime.utcnow(),
                    metadata=metadata
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error calculating {indicator}: {e}")
            return None
    
    async def _detect_patterns(self, symbol: str) -> List[PatternSignal]:
        """Detect chart patterns"""
        patterns = []
        
        for timeframe in self.timeframes:
            ohlcv_data = self._get_ohlcv_array(symbol, timeframe)
            
            if len(ohlcv_data) < 100:  # Need sufficient data for patterns
                continue
            
            # Detect various patterns
            patterns.extend(self._detect_candlestick_patterns(ohlcv_data, timeframe))
            patterns.extend(self._detect_chart_patterns(ohlcv_data, timeframe))
        
        return patterns
    
    def _detect_candlestick_patterns(
        self,
        ohlcv_data: np.ndarray,
        timeframe: TimeFrame
    ) -> List[PatternSignal]:
        """Detect candlestick patterns using TA-Lib"""
        patterns = []
        
        try:
            open_prices = ohlcv_data[:, 0]
            high_prices = ohlcv_data[:, 1]
            low_prices = ohlcv_data[:, 2]
            close_prices = ohlcv_data[:, 3]
            
            # Bullish patterns
            bullish_patterns = {
                "HAMMER": talib.CDLHAMMER,
                "INVERTED_HAMMER": talib.CDLINVERTEDHAMMER,
                "BULLISH_ENGULFING": talib.CDLENGULFING,
                "PIERCING_LINE": talib.CDLPIERCING,
                "MORNING_STAR": talib.CDLMORNINGSTAR,
                "THREE_WHITE_SOLDIERS": talib.CDL3WHITESOLDIERS,
            }
            
            # Bearish patterns
            bearish_patterns = {
                "HANGING_MAN": talib.CDLHANGINGMAN,
                "SHOOTING_STAR": talib.CDLSHOOTINGSTAR,
                "BEARISH_ENGULFING": talib.CDLENGULFING,
                "DARK_CLOUD_COVER": talib.CDLDARKCLOUDCOVER,
                "EVENING_STAR": talib.CDLEVENINGSTAR,
                "THREE_BLACK_CROWS": talib.CDL3BLACKCROWS,
            }
            
            # Check bullish patterns
            for pattern_name, pattern_func in bullish_patterns.items():
                result = pattern_func(open_prices, high_prices, low_prices, close_prices)
                
                if len(result) > 0 and result[-1] > 0:  # Bullish signal
                    pattern = PatternSignal(
                        pattern_name=pattern_name,
                        timeframe=timeframe,
                        pattern_type="bullish",
                        confidence=min(abs(result[-1]) / 100, 1.0),  # Normalize confidence
                        target_price=Decimal(str(close_prices[-1] * 1.02)),  # 2% target
                        stop_loss=Decimal(str(low_prices[-1] * 0.99))  # 1% below low
                    )
                    patterns.append(pattern)
            
            # Check bearish patterns
            for pattern_name, pattern_func in bearish_patterns.items():
                result = pattern_func(open_prices, high_prices, low_prices, close_prices)
                
                if len(result) > 0 and result[-1] < 0:  # Bearish signal
                    pattern = PatternSignal(
                        pattern_name=pattern_name,
                        timeframe=timeframe,
                        pattern_type="bearish",
                        confidence=min(abs(result[-1]) / 100, 1.0),
                        target_price=Decimal(str(close_prices[-1] * 0.98)),  # 2% target
                        stop_loss=Decimal(str(high_prices[-1] * 1.01))  # 1% above high
                    )
                    patterns.append(pattern)
            
        except Exception as e:
            self.logger.error(f"Error detecting candlestick patterns: {e}")
        
        return patterns
    
    def _detect_chart_patterns(
        self,
        ohlcv_data: np.ndarray,
        timeframe: TimeFrame
    ) -> List[PatternSignal]:
        """Detect chart patterns (triangles, channels, etc.)"""
        patterns = []
        
        try:
            high_prices = ohlcv_data[:, 1]
            low_prices = ohlcv_data[:, 2]
            close_prices = ohlcv_data[:, 3]
            
            # Simple support/resistance detection
            support, resistance = self._find_support_resistance(high_prices, low_prices, close_prices)
            
            current_price = close_prices[-1]
            
            # Check for breakout patterns
            if resistance and current_price > resistance * 1.01:
                pattern = PatternSignal(
                    pattern_name="RESISTANCE_BREAKOUT",
                    timeframe=timeframe,
                    pattern_type="bullish",
                    confidence=0.7,
                    target_price=Decimal(str(resistance * 1.05)),
                    stop_loss=Decimal(str(resistance * 0.99))
                )
                patterns.append(pattern)
            
            elif support and current_price < support * 0.99:
                pattern = PatternSignal(
                    pattern_name="SUPPORT_BREAKDOWN",
                    timeframe=timeframe,
                    pattern_type="bearish",
                    confidence=0.7,
                    target_price=Decimal(str(support * 0.95)),
                    stop_loss=Decimal(str(support * 1.01))
                )
                patterns.append(pattern)
            
            # Detect triangle patterns
            triangle_pattern = self._detect_triangle(high_prices, low_prices)
            if triangle_pattern:
                patterns.append(triangle_pattern)
            
        except Exception as e:
            self.logger.error(f"Error detecting chart patterns: {e}")
        
        return patterns
    
    def _find_support_resistance(
        self,
        high_prices: np.ndarray,
        low_prices: np.ndarray,
        close_prices: np.ndarray
    ) -> Tuple[Optional[float], Optional[float]]:
        """Find support and resistance levels"""
        if len(close_prices) < 20:
            return None, None
        
        # Simple method: use recent highs and lows
        recent_highs = high_prices[-20:]
        recent_lows = low_prices[-20:]
        
        # Resistance: recent high
        resistance = np.max(recent_highs)
        
        # Support: recent low
        support = np.min(recent_lows)
        
        return support, resistance
    
    def _detect_triangle(
        self,
        high_prices: np.ndarray,
        low_prices: np.ndarray
    ) -> Optional[PatternSignal]:
        """Detect triangle patterns"""
        if len(high_prices) < 50:
            return None
        
        # Simplified triangle detection
        # Check if highs are descending and lows are ascending
        recent_highs = high_prices[-20:]
        recent_lows = low_prices[-20:]
        
        # Linear regression on highs and lows
        x = np.arange(len(recent_highs))
        
        high_slope = np.polyfit(x, recent_highs, 1)[0]
        low_slope = np.polyfit(x, recent_lows, 1)[0]
        
        # Symmetrical triangle: converging lines
        if high_slope < 0 and low_slope > 0 and abs(high_slope + low_slope) < 0.001:
            return PatternSignal(
                pattern_name="SYMMETRICAL_TRIANGLE",
                timeframe=TimeFrame.H1,  # Default
                pattern_type="neutral",
                confidence=0.6
            )
        
        return None
    
    def _aggregate_signals(
        self,
        technical_signals: List[TechnicalSignal],
        pattern_signals: List[PatternSignal],
        ml_signal: Optional[Dict[str, Any]]
    ) -> Tuple[str, SignalStrength, float]:
        """Aggregate signals to determine direction and strength"""
        # Count directional signals
        buy_count = 0
        sell_count = 0
        total_strength = 0.0
        
        # Weight technical signals
        for signal in technical_signals:
            weight = signal.strength * 0.3  # 30% weight for technical
            if signal.signal_type == "BUY":
                buy_count += weight
            elif signal.signal_type == "SELL":
                sell_count += weight
            total_strength += signal.strength
        
        # Weight pattern signals
        for pattern in pattern_signals:
            weight = pattern.confidence * 0.3  # 30% weight for patterns
            if pattern.pattern_type == "bullish":
                buy_count += weight
            elif pattern.pattern_type == "bearish":
                sell_count += weight
            total_strength += pattern.confidence
        
        # Weight ML signal
        if ml_signal:
            ml_weight = 0.4  # 40% weight for ML
            direction = ml_signal.get("direction", "NEUTRAL")
            confidence = ml_signal.get("confidence", 0.5)
            
            if direction == "UP":
                buy_count += confidence * ml_weight
            elif direction == "DOWN":
                sell_count += confidence * ml_weight
            total_strength += confidence
        
        # Determine direction
        if buy_count > sell_count * 1.2:  # 20% margin
            direction = "LONG"
        elif sell_count > buy_count * 1.2:
            direction = "SHORT"
        else:
            direction = "NEUTRAL"
        
        # Calculate overall confidence
        total_signals = len(technical_signals) + len(pattern_signals) + (1 if ml_signal else 0)
        confidence = (buy_count + sell_count) / max(total_signals, 1)
        
        # Determine strength
        if confidence > 0.8:
            strength = SignalStrength.VERY_STRONG
        elif confidence > 0.6:
            strength = SignalStrength.STRONG
        elif confidence > 0.4:
            strength = SignalStrength.MEDIUM
        else:
            strength = SignalStrength.WEAK
        
        return direction, strength, confidence
    
    def _calculate_levels(
        self,
        direction: str,
        current_price: Decimal,
        technical_signals: List[TechnicalSignal],
        pattern_signals: List[PatternSignal]
    ) -> Tuple[Decimal, Optional[Decimal], Optional[Decimal]]:
        """Calculate entry, target, and stop loss levels"""
        entry_price = current_price
        
        # Check if patterns provide levels
        target_prices = []
        stop_losses = []
        
        for pattern in pattern_signals:
            if pattern.target_price:
                target_prices.append(pattern.target_price)
            if pattern.stop_loss:
                stop_losses.append(pattern.stop_loss)
        
        # Use pattern levels if available
        if target_prices:
            target_price = sum(target_prices) / len(target_prices)
        else:
            # Default targets based on ATR or fixed percentage
            if direction == "LONG":
                target_price = entry_price * Decimal("1.02")  # 2% target
            else:
                target_price = entry_price * Decimal("0.98")
        
        if stop_losses:
            stop_loss = sum(stop_losses) / len(stop_losses)
        else:
            # Default stops
            if direction == "LONG":
                stop_loss = entry_price * Decimal("0.99")  # 1% stop
            else:
                stop_loss = entry_price * Decimal("1.01")
        
        return entry_price, target_price, stop_loss
    
    def _calculate_risk_reward(
        self,
        entry: Decimal,
        target: Optional[Decimal],
        stop: Optional[Decimal]
    ) -> Optional[float]:
        """Calculate risk/reward ratio"""
        if not target or not stop:
            return None
        
        risk = abs(float(entry - stop))
        reward = abs(float(target - entry))
        
        if risk > 0:
            return reward / risk
        return None
    
    def _calculate_expected_return(
        self,
        entry: Decimal,
        target: Optional[Decimal],
        confidence: float
    ) -> float:
        """Calculate expected return based on confidence"""
        if not target:
            return 0.0
        
        potential_return = float((target - entry) / entry)
        expected_return = potential_return * confidence
        
        return expected_return
    
    def _calculate_timeframe_alignment(self, technical_signals: List[TechnicalSignal]) -> float:
        """Calculate how aligned signals are across timeframes"""
        if not technical_signals:
            return 0.0
        
        # Group by timeframe
        timeframe_signals = defaultdict(list)
        for signal in technical_signals:
            timeframe_signals[signal.timeframe].append(signal)
        
        # Check alignment
        alignments = []
        for tf, signals in timeframe_signals.items():
            buy_count = sum(1 for s in signals if s.signal_type == "BUY")
            sell_count = sum(1 for s in signals if s.signal_type == "SELL")
            total = len(signals)
            
            if total > 0:
                alignment = max(buy_count, sell_count) / total
                alignments.append(alignment)
        
        return np.mean(alignments) if alignments else 0.0
    
    def _validate_composite_signal(self, signal: CompositeSignal) -> bool:
        """Validate composite signal before conversion"""
        # Check confidence
        if signal.confidence < self.min_confidence:
            return False
        
        # Check timeframe alignment
        if signal.timeframe_alignment < self.min_timeframe_alignment:
            return False
        
        # Check risk/reward
        if signal.risk_reward_ratio and signal.risk_reward_ratio < 1.5:
            return False
        
        return True
    
    def _convert_to_trading_signal(self, composite: CompositeSignal) -> Signal:
        """Convert composite signal to trading signal"""
        # Determine signal type
        if composite.direction == "LONG":
            signal_type = SignalType.LONG
            side = "BUY"
        elif composite.direction == "SHORT":
            signal_type = SignalType.SHORT
            side = "SELL"
        else:
            return None
        
        # Calculate position size (simplified)
        # In production, would use Kelly criterion or risk-based sizing
        base_size = Decimal("1000")  # Base position size
        confidence_multiplier = Decimal(str(composite.confidence))
        quantity = base_size * confidence_multiplier
        
        return Signal(
            signal_id=str(uuid.uuid4()),
            strategy_id="signal_generator",
            symbol=composite.symbol,
            exchange="binance",  # Default, would be determined by strategy
            signal_type=signal_type,
            timestamp=composite.timestamp,
            side=side,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            entry_price=composite.entry_price,
            strength=composite.strength,
            confidence=Decimal(str(composite.confidence)),
            expected_return=Decimal(str(composite.expected_return)),
            risk=Decimal("0.01"),  # 1% risk default
            stop_loss=composite.stop_loss,
            take_profit=composite.target_price,
            time_limit=timedelta(minutes=30),
            model_predictions={
                "technical_signals": len(composite.technical_signals),
                "pattern_signals": len(composite.pattern_signals),
                "timeframe_alignment": composite.timeframe_alignment
            }
        )
    
    def _get_current_price(self, market_data: MarketData) -> Decimal:
        """Get current price from market data"""
        if market_data.ticker:
            return market_data.ticker.last
        return Decimal("0")
    
    async def _update_historical_data(self, symbol: str, market_data: MarketData) -> None:
        """Update historical OHLCV data"""
        # This would typically receive OHLCV data from market data feed
        # For now, simulate with ticker data
        if market_data.ticker:
            # Create synthetic OHLCV from ticker
            ohlcv = OHLCV(
                symbol=symbol,
                exchange="aggregate",
                timestamp=datetime.utcnow(),
                interval="1m",
                open=market_data.ticker.last,
                high=market_data.ticker.last * Decimal("1.001"),
                low=market_data.ticker.last * Decimal("0.999"),
                close=market_data.ticker.last,
                volume=market_data.ticker.volume_24h / Decimal("1440"),  # Avg per minute
                quote_volume=market_data.ticker.quote_volume_24h / Decimal("1440")
            )
            
            # Add to appropriate timeframe queues
            self.ohlcv_data[symbol][TimeFrame.M1].append(ohlcv)
            
            # Aggregate to higher timeframes
            await self._aggregate_timeframes(symbol)
    
    async def _aggregate_timeframes(self, symbol: str) -> None:
        """Aggregate lower timeframe data to higher timeframes"""
        # This would implement proper timeframe aggregation
        # For now, just copy M1 data to other timeframes as placeholder
        m1_data = self.ohlcv_data[symbol][TimeFrame.M1]
        
        if len(m1_data) >= 5:
            # Aggregate to M5
            self.ohlcv_data[symbol][TimeFrame.M5].append(m1_data[-1])
        
        if len(m1_data) >= 15:
            # Aggregate to M15
            self.ohlcv_data[symbol][TimeFrame.M15].append(m1_data[-1])
        
        if len(m1_data) >= 60:
            # Aggregate to H1
            self.ohlcv_data[symbol][TimeFrame.H1].append(m1_data[-1])
    
    def _get_ohlcv_array(self, symbol: str, timeframe: TimeFrame) -> np.ndarray:
        """Get OHLCV data as numpy array"""
        ohlcv_list = list(self.ohlcv_data[symbol][timeframe])
        
        if not ohlcv_list:
            return np.array([])
        
        # Convert to numpy array [open, high, low, close, volume]
        data = []
        for ohlcv in ohlcv_list:
            data.append([
                float(ohlcv.open),
                float(ohlcv.high),
                float(ohlcv.low),
                float(ohlcv.close),
                float(ohlcv.volume)
            ])
        
        return np.array(data)
    
    def _setup_indicators(self, config: Dict[str, Any]) -> Dict[IndicatorType, Dict[str, Dict]]:
        """Setup technical indicators configuration"""
        default_indicators = {
            IndicatorType.TREND: {
                "SMA": {"period": 20},
                "EMA": {"period": 12},
                "EMA_SLOW": {"period": 26},
            },
            IndicatorType.MOMENTUM: {
                "RSI": {"period": 14},
                "MACD": {"fast": 12, "slow": 26, "signal": 9},
                "STOCH": {"k_period": 14, "d_period": 3},
            },
            IndicatorType.VOLATILITY: {
                "BB": {"period": 20, "std_dev": 2},
                "ATR": {"period": 14},
            },
            IndicatorType.VOLUME: {
                # Volume indicators would be added here
            }
        }
        
        # Merge with custom config
        indicators = {}
        for ind_type in IndicatorType:
            indicators[ind_type] = config.get(ind_type.value, default_indicators.get(ind_type, {}))
        
        return indicators
    
    async def _signal_monitoring_loop(self) -> None:
        """Monitor active signals and performance"""
        while True:
            try:
                # Clean up old signals
                cutoff_time = datetime.utcnow() - timedelta(hours=1)
                
                expired_symbols = []
                for symbol, signal in self.active_signals.items():
                    if signal.timestamp < cutoff_time:
                        expired_symbols.append(symbol)
                
                for symbol in expired_symbols:
                    self.active_signals.pop(symbol, None)
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                self.logger.error(f"Error in signal monitoring: {e}")
                await asyncio.sleep(60)
    
    async def _performance_tracking_loop(self) -> None:
        """Track signal performance"""
        while True:
            try:
                # This would track actual signal performance
                # For now, just log statistics
                total_signals = len(self.signal_history)
                active_signals = len(self.active_signals)
                
                self.logger.info(f"Signal statistics - Total: {total_signals}, Active: {active_signals}")
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in performance tracking: {e}")
                await asyncio.sleep(300)
    
    def get_signal_statistics(self) -> Dict[str, Any]:
        """Get signal generation statistics"""
        return {
            "total_signals": len(self.signal_history),
            "active_signals": len(self.active_signals),
            "symbols_tracked": len(self.ohlcv_data),
            "timeframes": [tf.value for tf in self.timeframes],
            "indicators_enabled": sum(len(inds) for inds in self.indicators.values())
        }