import { spawn } from 'child_process';

console.log("Starting Python FastAPI server via Uvicorn...");

// Spawn uvicorn to run the python app
// We use 'python3' -m uvicorn to ensure we use the right python environment if path issues arise, 
// but 'uvicorn' should be in path if installed via packager_tool.
// Using 'uvicorn' directly.
const pythonProcess = spawn('uvicorn', ['app.main:app', '--host', '0.0.0.0', '--port', '5000', '--reload'], {
  stdio: 'inherit',
  env: { ...process.env, PYTHONUNBUFFERED: "1" }
});

pythonProcess.on('close', (code) => {
  console.log(`Python process exited with code ${code}`);
  process.exit(code || 0);
});

pythonProcess.on('error', (err) => {
  console.error('Failed to start Python process:', err);
  process.exit(1);
});

// Handle termination signals to kill the python process
process.on('SIGTERM', () => {
  pythonProcess.kill('SIGTERM');
  process.exit(0);
});

process.on('SIGINT', () => {
  pythonProcess.kill('SIGINT');
  process.exit(0);
});
