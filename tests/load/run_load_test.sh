#!/usr/bin/env bash
echo "Starting Locust load test..."
locust --headless -u 200 -r 20 --run-time 60s --host http://localhost:8000 --html report.html
echo "Test complete. Results saved to report.html"
