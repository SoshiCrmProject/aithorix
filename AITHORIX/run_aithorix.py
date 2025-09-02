#!/usr/bin/env python3
"""
AITHORIX QUANTUM ULTIMATE v100.0 - Startup Script
Quick launcher for the trading system
"""

import asyncio
import sys
import os
from datetime import datetime

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from aithorix_quantum_ultimate import AITHORIXQuantumUltimate, create_api

def print_banner():
    """Print startup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════════╗
    ║                 AITHORIX QUANTUM ULTIMATE v100.0                ║
    ║              Enterprise Trading System - PRODUCTION             ║
    ║                                                                  ║
    ║  🚀 Complete Trading System with ALL Features                    ║
    ║  🧠 175 ML Models | 📊 Real-time Analytics                      ║
    ║  ⚡ Sub-50ms Execution | 🔒 Enterprise Security                 ║
    ║                                                                  ║
    ║  Status: INITIALIZING...                                        ║
    ╚══════════════════════════════════════════════════════════════════╝
    """
    print(banner)
    print(f"🕒 Startup Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

def run_api_server():
    """Run the FastAPI server"""
    print("🌐 Starting AITHORIX QUANTUM ULTIMATE API Server...")
    print("🔗 Access the API at: http://localhost:8000")
    print("📚 API Documentation: http://localhost:8000/docs")
    print("🔍 Portfolio Status: http://localhost:8000/portfolio")
    print()
    
    try:
        import uvicorn
        app = create_api()
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info",
            reload=False
        )
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested by user")
    except Exception as e:
        print(f"❌ Error running API server: {e}")

async def run_trading_system():
    """Run the complete trading system"""
    print("🤖 Starting AITHORIX QUANTUM ULTIMATE Trading System...")
    
    system = AITHORIXQuantumUltimate()
    
    try:
        await system.start()
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested by user")
    except Exception as e:
        print(f"❌ Error running trading system: {e}")
    finally:
        await system.stop()

def main():
    """Main entry point"""
    print_banner()
    
    # Check command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        print("Select mode:")
        print("1. API Server (recommended)")
        print("2. Trading System Only")
        print("3. Test Mode")
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == "1":
            mode = "api"
        elif choice == "2":
            mode = "trading"
        elif choice == "3":
            mode = "test"
        else:
            mode = "api"  # Default
    
    if mode == "api":
        run_api_server()
    elif mode == "trading":
        asyncio.run(run_trading_system())
    elif mode == "test":
        print("🧪 Running test suite...")
        os.system("python test_aithorix.py")
    else:
        print(f"❌ Unknown mode: {mode}")
        print("Available modes: api, trading, test")
        sys.exit(1)

if __name__ == "__main__":
    main()