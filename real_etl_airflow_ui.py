#!/usr/bin/env python3
"""
Real ETL Airflow UI - Includes actual ETL pipeline execution
"""

from flask import Flask, render_template_string, jsonify, request
import sys
import os
import subprocess
import time

# Add dags to path
sys.path.append('dags')

app = Flask(__name__)

# HTML template with real ETL DAG
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title> ETL Airflow UI - AWS Production Pipeline</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            font-weight: 300;
            margin-bottom: 10px;
        }
        
        .header p {
            opacity: 0.9;
            font-size: 1.2em;
        }
        
        .content {
            padding: 30px;
        }
        
        .dag-card {
            background: white;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            margin-bottom: 30px;
            overflow: hidden;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        .dag-header {
            background: #f8f9fa;
            padding: 25px;
            border-bottom: 2px solid #e0e0e0;
        }
        
        .dag-title {
            font-size: 1.8em;
            color: #2c3e50;
            margin-bottom: 15px;
            font-weight: bold;
        }
        
        .dag-info {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .info-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            border-left: 5px solid #3498db;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        
        .info-label {
            font-weight: bold;
            color: #7f8c8d;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        
        .info-value {
            color: #2c3e50;
            font-size: 1.2em;
            font-weight: 500;
        }
        
        .run-button {
            background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%);
            color: white;
            border: none;
            padding: 15px 30px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 18px;
            font-weight: bold;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(39, 174, 96, 0.3);
        }
        
        .run-button:hover {
            background: linear-gradient(135deg, #229954 0%, #27ae60 100%);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(39, 174, 96, 0.4);
        }
        
        .run-button:disabled {
            background: #95a5a6;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }
        
        .real-etl-button {
            background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%);
            box-shadow: 0 4px 15px rgba(231, 76, 60, 0.3);
        }
        
        .real-etl-button:hover {
            background: linear-gradient(135deg, #c0392b 0%, #a93226 100%);
            box-shadow: 0 6px 20px rgba(231, 76, 60, 0.4);
        }
        
        .tasks-section {
            padding: 25px;
            background: #f8f9fa;
        }
        
        .tasks-title {
            font-size: 1.4em;
            color: #2c3e50;
            margin-bottom: 20px;
            font-weight: bold;
        }
        
        .task-item {
            background: white;
            padding: 15px 20px;
            margin-bottom: 10px;
            border-radius: 8px;
            border-left: 5px solid #27ae60;
            display: flex;
            justify-content: space-between;
            align-items: center;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        
        .task-name {
            font-weight: bold;
            color: #2c3e50;
            font-size: 1.1em;
        }
        
        .task-type {
            background: #3498db;
            color: white;
            padding: 8px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }
        
        .logs-section {
            background: white;
            border: 2px solid #2c3e50;
            border-radius: 10px;
            margin-top: 30px;
            overflow: hidden;
        }
        
        .logs-header {
            background: #2c3e50;
            color: white;
            padding: 20px;
            font-size: 1.4em;
            font-weight: bold;
        }
        
        .log-output {
            background: #2c3e50;
            color: #ecf0f1;
            padding: 25px;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.6;
            max-height: 500px;
            overflow-y: auto;
            white-space: pre-wrap;
        }
        
        .status-indicator {
            display: inline-block;
            padding: 8px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            margin-left: 10px;
        }
        
        .status-ready {
            background: #d4edda;
            color: #155724;
            border: 2px solid #27ae60;
        }
        
        .status-real {
            background: #f8d7da;
            color: #721c24;
            border: 2px solid #e74c3c;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌪️ Real ETL Airflow UI</h1>
            <p>AWS Production ETL Pipeline - Generates Actual Output Data</p>
        </div>
        
        <div class="content">
            <!-- Real ETL Pipeline DAG -->
            <div class="dag-card">
                <div class="dag-header">
                    <div class="dag-title">🚀 Real ETL Pipeline DAG</div>
                    <div class="dag-info">
                        <div class="info-card">
                            <div class="info-label">DAG ID</div>
                            <div class="info-value">real_etl_pipeline_dag</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Schedule</div>
                            <div class="info-value">@once</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Tasks</div>
                            <div class="info-value">5 Tasks</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Status</div>
                            <div class="info-value">
                                <span class="status-indicator status-real">🔥 Generates Real Data</span>
                            </div>
                        </div>
                    </div>
                    <button class="run-button real-etl-button" onclick="runDAG('real_etl_pipeline_dag')">
                        🔥 Run Real ETL Pipeline
                    </button>
                </div>
                <div class="tasks-section">
                    <div class="tasks-title">📋 Real ETL Tasks (5 Total)</div>
                    <div class="task-item">
                        <span class="task-name">1. Data Ingestion</span>
                        <span class="task-type">BashOperator</span>
                    </div>
                    <div class="task-item">
                        <span class="task-name">2. Real ETL Processing</span>
                        <span class="task-type">PythonOperator</span>
                    </div>
                    <div class="task-item">
                        <span class="task-name">3. Check Output Data</span>
                        <span class="task-type">PythonOperator</span>
                    </div>
                    <div class="task-item">
                        <span class="task-name">4. Data Quality Checks</span>
                        <span class="task-type">PythonOperator</span>
                    </div>
                    <div class="task-item">
                        <span class="task-name">5. Pipeline Summary</span>
                        <span class="task-type">BashOperator</span>
                    </div>
                </div>
            </div>
            
            <!-- Demo Pipeline DAG (Simulation) -->
            <div class="dag-card">
                <div class="dag-header">
                    <div class="dag-title">🎭 Demo Pipeline DAG</div>
                    <div class="dag-info">
                        <div class="info-card">
                            <div class="info-label">DAG ID</div>
                            <div class="info-value">demo_pipeline_dag</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Schedule</div>
                            <div class="info-value">@daily</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Tasks</div>
                            <div class="info-value">7 Tasks</div>
                        </div>
                        <div class="info-card">
                            <div class="info-label">Status</div>
                            <div class="info-value">
                                <span class="status-indicator status-ready">🎭 Simulation Only</span>
                            </div>
                        </div>
                    </div>
                    <button class="run-button" onclick="runDAG('demo_pipeline_dag')">
                        ▶️ Run Demo Pipeline
                    </button>
                </div>
            </div>
            
            <!-- Logs Section -->
            <div class="logs-section">
                <div class="logs-header">📋 Execution Logs</div>
                <div id="log-output" class="log-output">Ready to run real ETL pipeline. 

🔥 REAL ETL PIPELINE:
- Click "🔥 Run Real ETL Pipeline" to process actual data
- Generates real output files in data/lakehouse/
- Processes all 5 data sources with PySpark
- Creates Bronze, Silver, and Gold layers
- Runs actual data quality checks

🎭 DEMO PIPELINE:
- Click "▶️ Run Demo Pipeline" for simulation only
- No actual data processing
- Shows pipeline structure only</div>
            </div>
        </div>
    </div>

    <script>
        function runDAG(dagId) {
            const button = event.target;
            const originalText = button.textContent;
            button.textContent = '⏳ Running...';
            button.disabled = true;
            
            const logOutput = document.getElementById('log-output');
            logOutput.textContent = `🚀 Starting DAG: ${dagId}\n⏳ Initializing execution...\n\n`;
            
            fetch('/run_dag', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({dag_id: dagId})
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    logOutput.textContent += `✅ DAG ${dagId} completed successfully!\n`;
                    logOutput.textContent += `📊 Tasks completed: ${data.tasks_completed}\n`;
                    logOutput.textContent += `⏱️ Duration: ${data.duration} seconds\n`;
                    logOutput.textContent += `\n📋 Execution Summary:\n`;
                    logOutput.textContent += `========================\n`;
                    logOutput.textContent += data.output;
                    
                    if (dagId === 'real_etl_pipeline_dag') {
                        logOutput.textContent += `\n🔥 REAL DATA GENERATED!\n`;
                        logOutput.textContent += `📁 Check these directories for output files:\n`;
                        logOutput.textContent += `  - data/output_data/\n`;
                        logOutput.textContent += `  - data/lakehouse/bronze/\n`;
                        logOutput.textContent += `  - data/lakehouse/silver/\n`;
                        logOutput.textContent += `  - data/lakehouse/gold/\n`;
                    }
                } else {
                    logOutput.textContent += `❌ Error: ${data.message}\n`;
                }
                button.textContent = originalText;
                button.disabled = false;
                
                // Scroll to bottom of logs
                logOutput.scrollTop = logOutput.scrollHeight;
            })
            .catch(error => {
                logOutput.textContent += `❌ Network Error: ${error.message}\n`;
                button.textContent = originalText;
                button.disabled = false;
            });
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/run_dag', methods=['POST'])
def run_dag():
    data = request.get_json()
    dag_id = data.get('dag_id')
    
    start_time = time.time()
    
    try:
        # Set environment variable
        env = os.environ.copy()
        env['AIRFLOW__CORE__XCOM_BACKEND'] = 'airflow.models.xcom.BaseXCom'
        
        # Run the DAG using our simple runner
        result = subprocess.run([
            'python3', 'simple_dag_runner.py', '--dag', dag_id
        ], capture_output=True, text=True, env=env, cwd=os.getcwd())
        
        duration = round(time.time() - start_time, 2)
        
        if result.returncode == 0:
            # Count tasks from output
            tasks_completed = result.stdout.count('✅') - 1  # Subtract 1 for the final success message
            return jsonify({
                'status': 'success',
                'tasks_completed': tasks_completed,
                'duration': duration,
                'output': result.stdout
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result.stderr,
                'duration': duration
            })
    
    except Exception as e:
        duration = round(time.time() - start_time, 2)
        return jsonify({
            'status': 'error',
            'message': str(e),
            'duration': duration
        })

if __name__ == '__main__':
    print("🌪️ Starting Real ETL Airflow UI Server...")
    print("🌐 URL: http://localhost:8083")
    print("🎯 Features:")
    print("  - Real ETL pipeline execution")
    print("  - Generates actual output data")
    print("  - Demo pipeline simulation")
    print("  - Professional interface")
    print("")
    app.run(host='0.0.0.0', port=8083, debug=False)
