# Ensure test logs directory exists for all tests
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
os.makedirs('test_logs', exist_ok=True)