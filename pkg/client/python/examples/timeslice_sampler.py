import argparse
import os
import time
import sys
import logging
from timeslice import SnapshotAgentClient
import requests
import subprocess

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start_vllm_server(model, host="0.0.0.0", port=8000, tensor_parallel_size=1):
    """Starts a vLLM server using subprocess."""
    command = [
        "python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", model,
        "--host", host,
        "--port", str(port),
        "--tensor-parallel-size", str(tensor_parallel_size)
    ]
    logger.info(f"Starting vLLM server with command: {' '.join(command)}")
    return subprocess.Popen(command)


def wait_for_vllm_health(host, port, retries=180, delay=3):
    """Polls the vLLM /health endpoint until it returns 200 or retries are exhausted."""
    url = f"http://{host}:{port}/health"
    logger.info(f"Waiting for vLLM health check at {url}...")
    for i in range(retries):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                logger.info("vLLM is healthy!")
                return True
        except requests.RequestException:
            pass
        
        if i % 10 == 0 and i > 0:
            logger.info(f"Still waiting for vLLM health check... ({i}/{retries})")
        time.sleep(delay)
    
    logger.error(f"vLLM health check failed after {retries} retries.")
    sys.exit(1)


def call_vllm_generate(host, port, prompt, model):
    """Calls the generate endpoint of the vLLM server."""
    url = f"http://{host}:{port}/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "max_tokens": 16,
        "temperature": 0.0,
    }
    try:
        logger.info(f"Calling vLLM generate at {url} with prompt: {prompt}")
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"vLLM Response: {data}")
        return data
    except Exception as e:
        logger.warning(f"Error calling vLLM generate: {e}")
        # Fallback to OpenAI completions
        openai_url = f"http://{host}:{port}/v1/completions"
        try:
            logger.info(f"Retrying with OpenAI completions at {openai_url}")
            openai_payload = {
                "model": model,
                "prompt": prompt,
                "max_tokens": 16,
            }
            response = requests.post(openai_url, json=openai_payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            logger.info(f"vLLM (OpenAI) Response: {data}")
            return data
        except Exception as e2:
            logger.error(f"Error calling vLLM OpenAI completions: {e2}")
    return None


def run(endpoint, job_id, group, interval, vllm_host=None, vllm_port=None, prompt=None, model=None):
    logger.info(f"Starting sampler, calling {endpoint} every {interval}s")
    
    call_vllm_generate(vllm_host, vllm_port, prompt, model)
    
    with SnapshotAgentClient(endpoint) as client:
        resp = client.snapshot(job_id, group)
        if not resp:
            sys.exit(1)

        while True:
            complete = False
            if resp and resp.operation_id:
                # Poll for status
                time.sleep(1)
                operation = client.get_operation(resp.operation_id)
                if not operation:
                    sys.exit(1)

                from timeslice.snapshot_agent.snapshot_agent_pb2 import OPERATION_STATUS_COMPLETE, OPERATION_STATUS_FAILED
                
                if operation.status == OPERATION_STATUS_COMPLETE:
                    complete = True
                elif operation.status == OPERATION_STATUS_FAILED:
                    logger.error(f"Operation {resp.operation_id} failed")
                    sys.exit(1)
                else:
                    logger.info(f"Operation {resp.operation_id} not complete, status: {operation.status}")
            
            if vllm_host and vllm_port and prompt and complete:
                resp = client.restore(job_id, group)
                if not resp:
                    sys.exit(1)
                
                if resp and resp.operation_id:
                    logger.info(f"Restore Response: operation_id={resp.operation_id}")
                    complete = False
                    while not complete:
                        time.sleep(1)
                        operation = client.get_operation(resp.operation_id)
                        if not operation:
                            sys.exit(1)

                        if operation.status == OPERATION_STATUS_COMPLETE:
                            complete = True
                        elif operation.status == OPERATION_STATUS_FAILED:
                            logger.error(f"Operation {resp.operation_id} failed")
                            sys.exit(1)
                        else:
                            logger.info(f"Operation {resp.operation_id} not complete, status: {operation.status}")
                    
                    if complete:
                        wait_for_vllm_health(vllm_host, vllm_port)
                        logger.info(f"vLLM server healthy after restore")
                        call_vllm_generate(vllm_host, vllm_port, prompt, model)
                        resp = client.snapshot(job_id, group)
                        if not resp:
                            sys.exit(1)

            time.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sampler script using timeslice library")
    parser.add_argument("--endpoint", default=os.getenv("AGENT_ENDPOINT", "localhost:9001"), help="gRPC endpoint")
    parser.add_argument("--job-id", default=os.getenv("JOB_ID", "test-job"), help="Job ID for snapshot")
    parser.add_argument("--group", default=os.getenv("GROUP", "default"), help="Group for snapshot")
    parser.add_argument("--interval", type=int, default=int(os.getenv("INTERVAL", "30")), help="Interval in seconds")
    parser.add_argument("--vllm-model", default=os.getenv("VLLM_MODEL"), help="Model to start vLLM server with (optional)")
    parser.add_argument("--vllm-host", default=os.getenv("VLLM_HOST", "0.0.0.0"), help="vLLM server host")
    parser.add_argument("--vllm-port", type=int, default=int(os.getenv("VLLM_PORT", "8000")), help="vLLM server port")
    parser.add_argument("--prompt", default=os.getenv("PROMPT", "San Francisco is a"), help="Prompt for vLLM generate")
    parser.add_argument("--tensor-parallel-size", type=int, default=int(os.getenv("TENSOR_PARALLEL_SIZE", "1")), help="Tensor parallel size for vLLM")

    args = parser.parse_args()

    if args.vllm_model:
        vllm_process = start_vllm_server(args.vllm_model, args.vllm_host, args.vllm_port, args.tensor_parallel_size)
        wait_for_vllm_health(args.vllm_host, args.vllm_port)
        logger.info(f"vLLM server started and healthy with PID: {vllm_process.pid}")

    run(args.endpoint, args.job_id, args.group, args.interval, args.vllm_host, args.vllm_port, args.prompt, args.vllm_model)
