"""
AITHORIX Base Model - Production Ready Implementation
Complete base class for all 175 ML models with full functionality
"""

import os
import json
import time
import hashlib
import numpy as np
import torch
import torch.nn as nn
from typing import Dict, Any, List, Tuple, Optional, Union
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import joblib
import onnx
import onnxruntime as ort
from torch.cuda.amp import autocast, GradScaler
import tensorrt as trt
import pycuda.driver as cuda
import pycuda.autoinit
from collections import deque
import psutil
import GPUtil
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from dataclasses import dataclass, asdict
import yaml
import dill
import cloudpickle


@dataclass
class ModelMetrics:
    """Complete metrics tracking for model performance"""
    accuracy: float = 0.0
    precision: float = 0.0
    recall: float = 0.0
    f1_score: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_trade_duration: float = 0.0
    total_trades: int = 0
    profitable_trades: int = 0
    loss_trades: int = 0
    inference_time_ms: float = 0.0
    memory_usage_mb: float = 0.0
    last_updated: datetime = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['last_updated'] = self.last_updated.isoformat() if self.last_updated else None
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ModelMetrics':
        if 'last_updated' in data and data['last_updated']:
            data['last_updated'] = datetime.fromisoformat(data['last_updated'])
        return cls(**data)


@dataclass
class ModelConfig:
    """Complete configuration for model deployment"""
    model_id: str
    model_type: str
    version: str = "1.0.0"
    
    # Architecture parameters
    input_features: int = 100
    hidden_layers: List[int] = None
    output_features: int = 3
    dropout_rate: float = 0.2
    activation: str = "relu"
    
    # Training parameters
    learning_rate: float = 0.001
    batch_size: int = 32
    epochs: int = 100
    early_stopping_patience: int = 10
    gradient_clip_norm: float = 1.0
    weight_decay: float = 0.0001
    
    # Optimization parameters
    optimizer: str = "adam"
    scheduler: str = "cosine"
    warmup_steps: int = 1000
    
    # Hardware optimization
    use_cuda: bool = True
    use_mixed_precision: bool = True
    use_tensorrt: bool = True
    use_onnx: bool = True
    batch_inference: bool = True
    max_batch_size: int = 128
    
    # Memory optimization
    gradient_checkpointing: bool = True
    cpu_offload: bool = False
    model_parallelism: bool = False
    
    # Production settings
    confidence_threshold: float = 0.85
    max_position_size: float = 0.05
    stop_loss: float = 0.02
    take_profit: float = 0.03
    
    # Monitoring
    log_predictions: bool = True
    alert_on_anomaly: bool = True
    performance_tracking: bool = True
    
    def __post_init__(self):
        if self.hidden_layers is None:
            self.hidden_layers = [256, 128, 64]
    
    def save(self, path: str):
        with open(path, 'w') as f:
            yaml.dump(asdict(self), f)
    
    @classmethod
    def load(cls, path: str) -> 'ModelConfig':
        with open(path, 'r') as f:
            data = yaml.safe_load(f)
        return cls(**data)


class TensorRTEngine:
    """TensorRT optimization engine for ultra-fast inference"""
    
    def __init__(self, onnx_path: str, config: ModelConfig):
        self.config = config
        self.engine = None
        self.context = None
        self.inputs = []
        self.outputs = []
        self.bindings = []
        self.stream = cuda.Stream()
        
        self._build_engine(onnx_path)
    
    def _build_engine(self, onnx_path: str):
        """Build TensorRT engine from ONNX model"""
        TRT_LOGGER = trt.Logger(trt.Logger.WARNING)
        builder = trt.Builder(TRT_LOGGER)
        network = builder.create_network(1 << int(trt.NetworkDefinitionCreationFlag.EXPLICIT_BATCH))
        parser = trt.OnnxParser(network, TRT_LOGGER)
        
        # Parse ONNX model
        with open(onnx_path, 'rb') as f:
            if not parser.parse(f.read()):
                for error in range(parser.num_errors):
                    print(parser.get_error(error))
                raise RuntimeError("Failed to parse ONNX model")
        
        # Configure builder
        config = builder.create_builder_config()
        config.max_workspace_size = 1 << 30  # 1GB
        config.set_flag(trt.BuilderFlag.FP16)  # Enable FP16
        config.set_flag(trt.BuilderFlag.STRICT_TYPES)
        
        # Dynamic shapes
        profile = builder.create_optimization_profile()
        for i in range(network.num_inputs):
            input_shape = network.get_input(i).shape
            min_shape = [1] + list(input_shape[1:])
            opt_shape = [self.config.batch_size] + list(input_shape[1:])
            max_shape = [self.config.max_batch_size] + list(input_shape[1:])
            profile.set_shape(network.get_input(i).name, min_shape, opt_shape, max_shape)
        config.add_optimization_profile(profile)
        
        # Build engine
        self.engine = builder.build_engine(network, config)
        self.context = self.engine.create_execution_context()
        
        # Allocate buffers
        for binding in self.engine:
            shape = self.engine.get_binding_shape(binding)
            size = trt.volume(shape) * self.config.max_batch_size
            dtype = trt.nptype(self.engine.get_binding_dtype(binding))
            
            # Allocate host and device buffers
            host_mem = cuda.pagelocked_empty(size, dtype)
            device_mem = cuda.mem_alloc(host_mem.nbytes)
            
            # Store buffer info
            self.bindings.append(int(device_mem))
            if self.engine.binding_is_input(binding):
                self.inputs.append({'host': host_mem, 'device': device_mem})
            else:
                self.outputs.append({'host': host_mem, 'device': device_mem})
    
    def infer(self, input_data: np.ndarray) -> np.ndarray:
        """Run inference with TensorRT engine"""
        # Copy input to device
        np.copyto(self.inputs[0]['host'], input_data.ravel())
        cuda.memcpy_htod_async(self.inputs[0]['device'], self.inputs[0]['host'], self.stream)
        
        # Run inference
        self.context.execute_async_v2(bindings=self.bindings, stream_handle=self.stream.handle)
        
        # Copy output to host
        cuda.memcpy_dtoh_async(self.outputs[0]['host'], self.outputs[0]['device'], self.stream)
        self.stream.synchronize()
        
        return self.outputs[0]['host'].reshape(-1, self.config.output_features)


class BaseModel(ABC, nn.Module):
    """Production-ready base model with complete functionality"""
    
    def __init__(self, config: ModelConfig):
        super().__init__()
        self.config = config
        self.device = self._setup_device()
        self.metrics = ModelMetrics()
        self.logger = self._setup_logger()
        
        # Performance tracking
        self.inference_times = deque(maxlen=1000)
        self.prediction_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Model versioning
        self.model_hash = None
        self.last_update = datetime.now()
        
        # Hardware optimization
        self.tensorrt_engine = None
        self.onnx_session = None
        self.mixed_precision_scaler = GradScaler() if config.use_mixed_precision else None
        
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Initialize model architecture
        self._build_model()
        
        # Move to device
        self.to(self.device)
        
        # Initialize optimizers
        self._setup_optimization()
    
    def _setup_device(self) -> torch.device:
        """Setup optimal device with fallbacks"""
        if self.config.use_cuda and torch.cuda.is_available():
            # Select GPU with most free memory
            gpus = GPUtil.getGPUs()
            if gpus:
                best_gpu = max(gpus, key=lambda x: x.memoryFree)
                device = torch.device(f'cuda:{best_gpu.id}')
                torch.cuda.set_device(device)
                
                # Enable TF32 for A100 GPUs
                if torch.cuda.get_device_capability()[0] >= 8:
                    torch.backends.cuda.matmul.allow_tf32 = True
                    torch.backends.cudnn.allow_tf32 = True
                
                # Enable cuDNN autotuner
                torch.backends.cudnn.benchmark = True
                torch.backends.cudnn.deterministic = False
                
                self.logger.info(f"Using GPU: {best_gpu.name} with {best_gpu.memoryFree}MB free")
                return device
        
        self.logger.info("Using CPU device")
        return torch.device('cpu')
    
    def _setup_logger(self) -> logging.Logger:
        """Setup comprehensive logging"""
        logger = logging.getLogger(f"AITHORIX.{self.config.model_id}")
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_format)
        logger.addHandler(console_handler)
        
        # File handler
        os.makedirs('logs/models', exist_ok=True)
        file_handler = logging.FileHandler(f'logs/models/{self.config.model_id}.log')
        file_handler.setFormatter(console_format)
        logger.addHandler(file_handler)
        
        return logger
    
    @abstractmethod
    def _build_model(self):
        """Build model architecture - must be implemented by subclasses"""
        pass
    
    def _setup_optimization(self):
        """Setup optimizers and schedulers"""
        # Optimizer selection
        if self.config.optimizer == 'adam':
            self.optimizer = torch.optim.Adam(
                self.parameters(),
                lr=self.config.learning_rate,
                weight_decay=self.config.weight_decay,
                betas=(0.9, 0.999),
                eps=1e-8
            )
        elif self.config.optimizer == 'adamw':
            self.optimizer = torch.optim.AdamW(
                self.parameters(),
                lr=self.config.learning_rate,
                weight_decay=self.config.weight_decay
            )
        elif self.config.optimizer == 'sgd':
            self.optimizer = torch.optim.SGD(
                self.parameters(),
                lr=self.config.learning_rate,
                momentum=0.9,
                weight_decay=self.config.weight_decay,
                nesterov=True
            )
        
        # Scheduler selection
        if self.config.scheduler == 'cosine':
            self.scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
                self.optimizer,
                T_max=self.config.epochs,
                eta_min=1e-6
            )
        elif self.config.scheduler == 'plateau':
            self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
                self.optimizer,
                mode='min',
                factor=0.5,
                patience=5,
                verbose=True
            )
        elif self.config.scheduler == 'exponential':
            self.scheduler = torch.optim.lr_scheduler.ExponentialLR(
                self.optimizer,
                gamma=0.95
            )
    
    def preprocess(self, data: Union[np.ndarray, pd.DataFrame, torch.Tensor]) -> torch.Tensor:
        """Comprehensive data preprocessing"""
        # Convert to numpy if needed
        if isinstance(data, pd.DataFrame):
            data = data.values
        elif isinstance(data, torch.Tensor):
            data = data.cpu().numpy()
        
        # Data validation
        if np.any(np.isnan(data)):
            self.logger.warning("NaN values detected in input data")
            data = np.nan_to_num(data, nan=0.0)
        
        # Normalization (using running statistics)
        if not hasattr(self, 'running_mean'):
            self.running_mean = np.zeros(data.shape[-1])
            self.running_std = np.ones(data.shape[-1])
            self.n_samples = 0
        
        # Update running statistics
        batch_mean = np.mean(data, axis=0)
        batch_std = np.std(data, axis=0) + 1e-8
        
        momentum = 0.1
        self.running_mean = (1 - momentum) * self.running_mean + momentum * batch_mean
        self.running_std = (1 - momentum) * self.running_std + momentum * batch_std
        
        # Normalize
        data = (data - self.running_mean) / self.running_std
        
        # Convert to tensor
        tensor = torch.FloatTensor(data).to(self.device)
        
        # Add batch dimension if needed
        if len(tensor.shape) == 1:
            tensor = tensor.unsqueeze(0)
        
        return tensor
    
    @torch.no_grad()
    def predict(self, data: Union[np.ndarray, pd.DataFrame, torch.Tensor], 
                return_confidence: bool = True) -> Dict[str, Any]:
        """Production-ready prediction with caching and optimization"""
        start_time = time.time()
        
        # Generate cache key
        cache_key = self._generate_cache_key(data)
        
        # Check cache
        if cache_key in self.prediction_cache:
            self.cache_hits += 1
            cached_result = self.prediction_cache[cache_key].copy()
            cached_result['from_cache'] = True
            return cached_result
        
        self.cache_misses += 1
        
        # Preprocess data
        input_tensor = self.preprocess(data)
        
        # Run inference based on optimization level
        if self.config.use_tensorrt and self.tensorrt_engine:
            output = self._tensorrt_inference(input_tensor)
        elif self.config.use_onnx and self.onnx_session:
            output = self._onnx_inference(input_tensor)
        else:
            output = self._pytorch_inference(input_tensor)
        
        # Process predictions
        predictions = self._process_predictions(output)
        
        # Calculate confidence
        confidence = self._calculate_confidence(output)
        
        # Track inference time
        inference_time = (time.time() - start_time) * 1000  # ms
        self.inference_times.append(inference_time)
        
        # Prepare result
        result = {
            'prediction': predictions,
            'confidence': confidence,
            'inference_time_ms': inference_time,
            'timestamp': datetime.now().isoformat(),
            'model_version': self.config.version,
            'from_cache': False
        }
        
        if return_confidence:
            result['confidence_scores'] = self._get_confidence_scores(output)
        
        # Cache result
        self.prediction_cache[cache_key] = result.copy()
        
        # Limit cache size
        if len(self.prediction_cache) > 10000:
            # Remove oldest entries
            for key in list(self.prediction_cache.keys())[:1000]:
                del self.prediction_cache[key]
        
        return result
    
    def _generate_cache_key(self, data: Union[np.ndarray, pd.DataFrame, torch.Tensor]) -> str:
        """Generate unique cache key for input data"""
        if isinstance(data, pd.DataFrame):
            data = data.values
        elif isinstance(data, torch.Tensor):
            data = data.cpu().numpy()
        
        # Use hash of data for cache key
        data_bytes = data.tobytes()
        return hashlib.md5(data_bytes).hexdigest()
    
    def _pytorch_inference(self, input_tensor: torch.Tensor) -> torch.Tensor:
        """Standard PyTorch inference with mixed precision"""
        self.eval()
        
        if self.config.use_mixed_precision and self.device.type == 'cuda':
            with autocast():
                output = self.forward(input_tensor)
        else:
            output = self.forward(input_tensor)
        
        return output
    
    def _onnx_inference(self, input_tensor: torch.Tensor) -> torch.Tensor:
        """ONNX Runtime inference"""
        input_numpy = input_tensor.cpu().numpy()
        
        ort_inputs = {
            self.onnx_session.get_inputs()[0].name: input_numpy
        }
        
        ort_outputs = self.onnx_session.run(None, ort_inputs)
        
        return torch.from_numpy(ort_outputs[0]).to(self.device)
    
    def _tensorrt_inference(self, input_tensor: torch.Tensor) -> torch.Tensor:
        """TensorRT inference for maximum speed"""
        input_numpy = input_tensor.cpu().numpy()
        output_numpy = self.tensorrt_engine.infer(input_numpy)
        return torch.from_numpy(output_numpy).to(self.device)
    
    @abstractmethod
    def _process_predictions(self, output: torch.Tensor) -> Any:
        """Process raw model output into predictions"""
        pass
    
    @abstractmethod
    def _calculate_confidence(self, output: torch.Tensor) -> float:
        """Calculate prediction confidence"""
        pass
    
    @abstractmethod
    def _get_confidence_scores(self, output: torch.Tensor) -> Dict[str, float]:
        """Get detailed confidence scores"""
        pass
    
    def train_step(self, batch: Dict[str, torch.Tensor]) -> Dict[str, float]:
        """Single training step with mixed precision"""
        self.train()
        
        inputs = batch['inputs'].to(self.device)
        targets = batch['targets'].to(self.device)
        
        # Mixed precision training
        if self.config.use_mixed_precision:
            with autocast():
                outputs = self.forward(inputs)
                loss = self.calculate_loss(outputs, targets)
            
            self.optimizer.zero_grad()
            self.mixed_precision_scaler.scale(loss).backward()
            
            # Gradient clipping
            if self.config.gradient_clip_norm > 0:
                self.mixed_precision_scaler.unscale_(self.optimizer)
                torch.nn.utils.clip_grad_norm_(self.parameters(), self.config.gradient_clip_norm)
            
            self.mixed_precision_scaler.step(self.optimizer)
            self.mixed_precision_scaler.update()
        else:
            outputs = self.forward(inputs)
            loss = self.calculate_loss(outputs, targets)
            
            self.optimizer.zero_grad()
            loss.backward()
            
            # Gradient clipping
            if self.config.gradient_clip_norm > 0:
                torch.nn.utils.clip_grad_norm_(self.parameters(), self.config.gradient_clip_norm)
            
            self.optimizer.step()
        
        # Calculate metrics
        metrics = self._calculate_metrics(outputs, targets)
        metrics['loss'] = loss.item()
        
        return metrics
    
    @abstractmethod
    def calculate_loss(self, outputs: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        """Calculate loss - must be implemented by subclasses"""
        pass
    
    def _calculate_metrics(self, outputs: torch.Tensor, targets: torch.Tensor) -> Dict[str, float]:
        """Calculate comprehensive metrics"""
        with torch.no_grad():
            # Convert to predictions
            predictions = self._process_predictions(outputs)
            
            # Accuracy
            if isinstance(predictions, torch.Tensor):
                correct = (predictions == targets).float().mean()
                accuracy = correct.item()
            else:
                accuracy = 0.0
            
            # Additional metrics can be calculated here
            metrics = {
                'accuracy': accuracy,
                'learning_rate': self.optimizer.param_groups[0]['lr']
            }
        
        return metrics
    
    def save(self, path: str, include_optimizer: bool = True):
        """Save model with all necessary components"""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        # Prepare save dict
        save_dict = {
            'model_state_dict': self.state_dict(),
            'config': asdict(self.config),
            'metrics': self.metrics.to_dict(),
            'running_mean': self.running_mean if hasattr(self, 'running_mean') else None,
            'running_std': self.running_std if hasattr(self, 'running_std') else None,
            'model_hash': self._calculate_model_hash(),
            'last_update': self.last_update.isoformat()
        }
        
        if include_optimizer and hasattr(self, 'optimizer'):
            save_dict['optimizer_state_dict'] = self.optimizer.state_dict()
            save_dict['scheduler_state_dict'] = self.scheduler.state_dict() if hasattr(self, 'scheduler') else None
            if self.mixed_precision_scaler:
                save_dict['scaler_state_dict'] = self.mixed_precision_scaler.state_dict()
        
        # Save with multiple formats
        torch.save(save_dict, path)
        
        # Save ONNX version
        if self.config.use_onnx:
            self._export_onnx(path.replace('.pt', '.onnx'))
        
        # Save TensorRT version
        if self.config.use_tensorrt and self.device.type == 'cuda':
            self._export_tensorrt(path.replace('.pt', '.trt'))
        
        self.logger.info(f"Model saved to {path}")
    
    def load(self, path: str, load_optimizer: bool = True):
        """Load model with all components"""
        checkpoint = torch.load(path, map_location=self.device)
        
        # Load model state
        self.load_state_dict(checkpoint['model_state_dict'])
        
        # Load config
        self.config = ModelConfig(**checkpoint['config'])
        
        # Load metrics
        self.metrics = ModelMetrics.from_dict(checkpoint['metrics'])
        
        # Load normalization parameters
        if 'running_mean' in checkpoint and checkpoint['running_mean'] is not None:
            self.running_mean = checkpoint['running_mean']
            self.running_std = checkpoint['running_std']
        
        # Load optimizer state
        if load_optimizer and 'optimizer_state_dict' in checkpoint:
            self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
            if 'scheduler_state_dict' in checkpoint and checkpoint['scheduler_state_dict']:
                self.scheduler.load_state_dict(checkpoint['scheduler_state_dict'])
            if 'scaler_state_dict' in checkpoint and self.mixed_precision_scaler:
                self.mixed_precision_scaler.load_state_dict(checkpoint['scaler_state_dict'])
        
        # Update metadata
        self.model_hash = checkpoint.get('model_hash')
        self.last_update = datetime.fromisoformat(checkpoint.get('last_update', datetime.now().isoformat()))
        
        # Load optimized versions
        onnx_path = path.replace('.pt', '.onnx')
        if os.path.exists(onnx_path) and self.config.use_onnx:
            self._load_onnx(onnx_path)
        
        trt_path = path.replace('.pt', '.trt')
        if os.path.exists(trt_path) and self.config.use_tensorrt:
            self._load_tensorrt(trt_path)
        
        self.logger.info(f"Model loaded from {path}")
    
    def _export_onnx(self, path: str):
        """Export model to ONNX format"""
        self.eval()
        
        # Create dummy input
        dummy_input = torch.randn(1, self.config.input_features).to(self.device)
        
        # Export
        torch.onnx.export(
            self,
            dummy_input,
            path,
            export_params=True,
            opset_version=13,
            do_constant_folding=True,
            input_names=['input'],
            output_names=['output'],
            dynamic_axes={
                'input': {0: 'batch_size'},
                'output': {0: 'batch_size'}
            }
        )
        
        # Verify
        onnx_model = onnx.load(path)
        onnx.checker.check_model(onnx_model)
        
        # Create ONNX Runtime session
        self.onnx_session = ort.InferenceSession(
            path,
            providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
        )
    
    def _load_onnx(self, path: str):
        """Load ONNX model"""
        self.onnx_session = ort.InferenceSession(
            path,
            providers=['CUDAExecutionProvider', 'CPUExecutionProvider']
        )
    
    def _export_tensorrt(self, onnx_path: str):
        """Export model to TensorRT"""
        if not onnx_path.endswith('.onnx'):
            onnx_path = onnx_path.replace('.trt', '.onnx')
        
        if not os.path.exists(onnx_path):
            self._export_onnx(onnx_path)
        
        self.tensorrt_engine = TensorRTEngine(onnx_path, self.config)
    
    def _load_tensorrt(self, path: str):
        """Load TensorRT engine"""
        # TensorRT engines are rebuilt from ONNX
        onnx_path = path.replace('.trt', '.onnx')
        if os.path.exists(onnx_path):
            self.tensorrt_engine = TensorRTEngine(onnx_path, self.config)
    
    def _calculate_model_hash(self) -> str:
        """Calculate unique hash for model state"""
        hasher = hashlib.sha256()
        
        for param in self.parameters():
            hasher.update(param.data.cpu().numpy().tobytes())
        
        return hasher.hexdigest()
    
    def get_memory_usage(self) -> Dict[str, float]:
        """Get detailed memory usage statistics"""
        memory_stats = {
            'model_parameters_mb': sum(p.numel() * p.element_size() for p in self.parameters()) / 1024 / 1024,
            'model_buffers_mb': sum(b.numel() * b.element_size() for b in self.buffers()) / 1024 / 1024,
            'cache_size': len(self.prediction_cache),
            'cache_hit_rate': self.cache_hits / max(self.cache_hits + self.cache_misses, 1)
        }
        
        if self.device.type == 'cuda':
            memory_stats['gpu_allocated_mb'] = torch.cuda.memory_allocated(self.device) / 1024 / 1024
            memory_stats['gpu_reserved_mb'] = torch.cuda.memory_reserved(self.device) / 1024 / 1024
        
        process = psutil.Process()
        memory_stats['process_memory_mb'] = process.memory_info().rss / 1024 / 1024
        
        return memory_stats
    
    def get_performance_stats(self) -> Dict[str, float]:
        """Get comprehensive performance statistics"""
        if not self.inference_times:
            return {}
        
        inference_array = np.array(self.inference_times)
        
        return {
            'avg_inference_time_ms': np.mean(inference_array),
            'p50_inference_time_ms': np.percentile(inference_array, 50),
            'p95_inference_time_ms': np.percentile(inference_array, 95),
            'p99_inference_time_ms': np.percentile(inference_array, 99),
            'max_inference_time_ms': np.max(inference_array),
            'min_inference_time_ms': np.min(inference_array),
            'total_predictions': len(self.inference_times),
            'cache_hit_rate': self.cache_hits / max(self.cache_hits + self.cache_misses, 1)
        }
    
    def update_metrics(self, predictions: np.ndarray, targets: np.ndarray, 
                      profits: Optional[np.ndarray] = None):
        """Update model metrics with new results"""
        # Classification metrics
        if len(predictions.shape) == 1:
            correct = predictions == targets
            self.metrics.accuracy = np.mean(correct)
            
            # Trading metrics
            if profits is not None:
                profitable = profits > 0
                self.metrics.win_rate = np.mean(profitable)
                self.metrics.profit_factor = np.sum(profits[profitable]) / np.abs(np.sum(profits[~profitable]))
                self.metrics.total_trades = len(profits)
                self.metrics.profitable_trades = np.sum(profitable)
                self.metrics.loss_trades = np.sum(~profitable)
                
                # Calculate Sharpe ratio
                if len(profits) > 1:
                    returns = np.diff(profits) / profits[:-1]
                    self.metrics.sharpe_ratio = np.mean(returns) / (np.std(returns) + 1e-8) * np.sqrt(252)
                
                # Calculate max drawdown
                cumulative = np.cumsum(profits)
                running_max = np.maximum.accumulate(cumulative)
                drawdown = (cumulative - running_max) / (running_max + 1e-8)
                self.metrics.max_drawdown = np.min(drawdown)
        
        # Update performance metrics
        self.metrics.inference_time_ms = np.mean(self.inference_times) if self.inference_times else 0
        self.metrics.memory_usage_mb = self.get_memory_usage()['model_parameters_mb']
        self.metrics.last_updated = datetime.now()
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.config.model_id}, version={self.config.version})"