"""
AITHORIX Trading System Setup Configuration
Production-ready package installation and distribution
"""

from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize
import numpy as np
from pathlib import Path
import sys
import os

# Read version from package
VERSION = "1.0.0"

# Read long description from README
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Cython extensions for performance-critical components
extensions = [
    Extension(
        "aithorix.core.engine.fast_math",
        ["aithorix/core/engine/fast_math.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3", "-march=native", "-ffast-math"],
    ),
    Extension(
        "aithorix.models.directional.lstm.fast_lstm",
        ["aithorix/models/directional/lstm/fast_lstm.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3", "-march=native"],
    ),
    Extension(
        "aithorix.exchanges.common.fast_orderbook",
        ["aithorix/exchanges/common/fast_orderbook.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=["-O3", "-march=native"],
    ),
]

# Core dependencies
install_requires = [
    # Core Python packages
    "numpy>=1.24.0,<2.0.0",
    "pandas>=2.0.0,<3.0.0",
    "scipy>=1.10.0,<2.0.0",
    "numba>=0.57.0",
    "cython>=3.0.0",
    
    # ML/AI frameworks
    "torch>=2.0.0",
    "tensorflow>=2.13.0",
    "scikit-learn>=1.3.0",
    "xgboost>=2.0.0",
    "lightgbm>=4.0.0",
    "catboost>=1.2.0",
    "optuna>=3.3.0",
    "transformers>=4.30.0",
    "prophet>=1.1.0",
    
    # Trading specific
    "ccxt>=4.0.0",
    "ta>=0.11.0",
    "pyalgotrade>=0.20",
    "backtrader>=1.9.76.123",
    "quantlib>=1.30",
    
    # Web frameworks
    "fastapi>=0.100.0",
    "uvicorn[standard]>=0.23.0",
    "websockets>=11.0",
    "httpx>=0.24.0",
    "aiohttp>=3.8.0",
    
    # Database
    "sqlalchemy>=2.0.0",
    "asyncpg>=0.28.0",
    "psycopg2-binary>=2.9.0",
    "redis>=5.0.0",
    "motor>=3.3.0",
    "pymongo>=4.5.0",
    
    # Data processing
    "pyarrow>=13.0.0",
    "vaex>=4.17.0",
    "dask>=2023.8.0",
    "ray>=2.6.0",
    "polars>=0.19.0",
    
    # Messaging
    "aiokafka>=0.8.0",
    "confluent-kafka>=2.2.0",
    "celery>=5.3.0",
    "kombu>=5.3.0",
    
    # Monitoring
    "prometheus-client>=0.17.0",
    "opentelemetry-api>=1.20.0",
    "opentelemetry-sdk>=1.20.0",
    "opentelemetry-instrumentation-fastapi>=0.41b0",
    
    # Security
    "cryptography>=41.0.0",
    "pyjwt>=2.8.0",
    "passlib[bcrypt]>=1.7.4",
    "python-multipart>=0.0.6",
    "python-jose[cryptography]>=3.3.0",
    
    # Configuration
    "pydantic>=2.0.0",
    "pydantic-settings>=2.0.0",
    "python-dotenv>=1.0.0",
    "omegaconf>=2.3.0",
    "hydra-core>=1.3.0",
    
    # Utilities
    "click>=8.1.0",
    "rich>=13.5.0",
    "typer>=0.9.0",
    "structlog>=23.1.0",
    "loguru>=0.7.0",
    "python-json-logger>=2.0.7",
    "croniter>=1.4.0",
    "schedule>=1.2.0",
    "tenacity>=8.2.0",
    "cachetools>=5.3.0",
    
    # Testing (also in dev but needed for some runtime checks)
    "hypothesis>=6.82.0",
    "faker>=19.3.0",
]

# Development dependencies
extras_require = {
    "dev": [
        # Testing
        "pytest>=7.4.0",
        "pytest-asyncio>=0.21.0",
        "pytest-cov>=4.1.0",
        "pytest-mock>=3.11.0",
        "pytest-benchmark>=4.0.0",
        "pytest-timeout>=2.1.0",
        "pytest-xdist>=3.3.0",
        "tox>=4.6.0",
        
        # Code quality
        "black>=23.7.0",
        "isort>=5.12.0",
        "flake8>=6.1.0",
        "flake8-docstrings>=1.7.0",
        "flake8-annotations>=3.0.0",
        "flake8-bugbear>=23.7.0",
        "mypy>=1.5.0",
        "pylint>=2.17.0",
        "bandit>=1.7.0",
        "safety>=2.3.0",
        
        # Documentation
        "sphinx>=7.1.0",
        "sphinx-rtd-theme>=1.3.0",
        "sphinx-autodoc-typehints>=1.24.0",
        "myst-parser>=2.0.0",
        
        # Development tools
        "ipython>=8.14.0",
        "jupyter>=1.0.0",
        "jupyterlab>=4.0.0",
        "notebook>=7.0.0",
        "pre-commit>=3.3.0",
        "commitizen>=3.5.0",
    ],
    "research": [
        # Additional research tools
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
        "plotly>=5.15.0",
        "dash>=2.11.0",
        "bokeh>=3.2.0",
        "holoviews>=1.17.0",
        "panel>=1.2.0",
        "streamlit>=1.25.0",
        "gradio>=3.40.0",
        "wandb>=0.15.0",
        "mlflow>=2.5.0",
        "tensorboard>=2.13.0",
    ],
}

# Console scripts
entry_points = {
    "console_scripts": [
        "aithorix=aithorix.cli:main",
        "aithorix-trading=aithorix.core.main:run_trading",
        "aithorix-backtest=aithorix.utils.backtest:main",
        "aithorix-monitor=aithorix.monitoring.monitor:main",
        "aithorix-risk=aithorix.models.risk.cli:main",
    ],
}

# Package data
package_data = {
    "aithorix": [
        "config/*.yaml",
        "config/**/*.yaml",
        "data/models/configs/*.yaml",
        "static/**/*",
        "templates/**/*",
    ],
}

# C extensions compilation
if sys.platform == "win32":
    extra_compile_args = ["/O2"]
    extra_link_args = []
else:
    extra_compile_args = ["-O3", "-march=native", "-ffast-math", "-fopenmp"]
    extra_link_args = ["-fopenmp"]

# Custom build commands
from setuptools.command.build_ext import build_ext
from setuptools.command.install import install

class CustomBuildExt(build_ext):
    """Custom build extension to handle Cython compilation"""
    
    def build_extensions(self):
        # Detect numpy include directory
        numpy_include = np.get_include()
        for ext in self.extensions:
            ext.include_dirs.append(numpy_include)
        
        # Add OpenMP support if available
        if self.compiler.compiler_type == "unix":
            for ext in self.extensions:
                ext.extra_compile_args.extend(["-fopenmp"])
                ext.extra_link_args.extend(["-fopenmp"])
        
        build_ext.build_extensions(self)

class CustomInstall(install):
    """Custom installation to handle post-install tasks"""
    
    def run(self):
        install.run(self)
        
        # Create necessary directories
        import pathlib
        dirs_to_create = [
            "logs/trading",
            "logs/system",
            "logs/audit",
            "data/cache",
            "data/models/weights",
        ]
        
        for dir_path in dirs_to_create:
            pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)
        
        print("\n✅ AITHORIX installation complete!")
        print("📚 Run 'aithorix --help' for usage information")

# Main setup configuration
setup(
    name="aithorix",
    version=VERSION,
    author="AITHORIX Team",
    author_email="team@aithorix.ai",
    description="Advanced AI Trading System with 175 ML Models",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aithorix/aithorix",
    project_urls={
        "Documentation": "https://docs.aithorix.ai",
        "Source": "https://github.com/aithorix/aithorix",
        "Tracker": "https://github.com/aithorix/aithorix/issues",
    },
    
    packages=find_packages(exclude=["tests", "tests.*", "docs", "scripts"]),
    include_package_data=True,
    package_data=package_data,
    
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.11",
    
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "embedsignature": True,
        },
        annotate=True,
    ) if not os.environ.get("AITHORIX_NO_CYTHON") else [],
    
    cmdclass={
        "build_ext": CustomBuildExt,
        "install": CustomInstall,
    },
    
    entry_points=entry_points,
    
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: Other/Proprietary License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Cython",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Environment :: GPU :: NVIDIA CUDA :: 12",
    ],
    
    keywords="trading cryptocurrency ai ml deeplearning hft arbitrage quant",
    zip_safe=False,
    
    # Testing
    test_suite="tests",
    tests_require=extras_require["dev"],
)