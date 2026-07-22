import os
import sys
import time
import torch
import shutil
import json
from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest
from vllm.lora.layers import BaseLayerWithLoRA

# Add path to client library
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../pkg/client/python')))
try:
    from timeslice.snapshot_agent.client import SnapshotAgentClient
    from timeslice.snapshot_agent import snapshot_agent_pb2
except ImportError:
    # Try container path
    sys.path.append('/app/pkg/client/python')
    from timeslice.snapshot_agent.client import SnapshotAgentClient
    from timeslice.snapshot_agent import snapshot_agent_pb2

def generate_dummy_lora_from_model(model, lora_dir, adapter_name):
    print(f"Generating dummy LoRA {adapter_name}...")
    os.makedirs(lora_dir, exist_ok=True)
    target_modules = []
    state_dict = {}
    rank = 16
    val_multiplier = 1.0 if adapter_name == "A" else 10.0 # Make them very different
    
    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            # Map packed modules to their sub-modules for PEFT naming compatibility
            sub_modules = []
            if name.endswith(".qkv_proj"):
                base_name = name[:-8] # remove "qkv_proj"
                sub_modules = [base_name + "q_proj", base_name + "v_proj"]
            elif name.endswith(".gate_up_proj"):
                base_name = name[:-12]
                sub_modules = [base_name + "gate_proj", base_name + "up_proj"]
            else:
                sub_modules = [name]
                
            for sub_name in sub_modules:
                suffix = sub_name.split(".")[-1]
                if suffix not in target_modules:
                    target_modules.append(suffix)
                
                input_dim = getattr(module, "input_size", None)
                output_dim = getattr(module, "output_size", None)
                if input_dim is None or output_dim is None:
                    base_layer = getattr(module, "base_layer", None)
                    if base_layer:
                        input_dim = getattr(base_layer, "input_size", getattr(base_layer, "in_features", None))
                        output_dim = getattr(base_layer, "output_size", getattr(base_layer, "out_features", None))
                
                if input_dim and output_dim:
                    sub_output_dim = output_dim
                    if name.endswith(".qkv_proj"):
                        sub_output_dim = output_dim // 3
                    elif name.endswith(".gate_up_proj"):
                        sub_output_dim = output_dim // 2
                        
                    key_a = f"base_model.model.{sub_name}.lora_A.weight"
                    key_b = f"base_model.model.{sub_name}.lora_B.weight"
                    # Use constant values to make output deterministic
                    state_dict[key_a] = torch.ones(rank, input_dim) * 0.01 * val_multiplier
                    state_dict[key_b] = torch.ones(sub_output_dim, rank) * 0.01 * val_multiplier
                
    config = {
      "r": rank,
      "lora_alpha": rank * 2,
      "target_modules": target_modules,
      "lora_dropout": 0.0,
      "bias": "none",
      "task_type": "CAUSAL_LM",
      "peft_type": "LORA",
      "base_model_name_or_path": "dummy"
    }
    with open(os.path.join(lora_dir, "adapter_config.json"), "w") as f:
        json.dump(config, f, indent=2)
    torch.save(state_dict, os.path.join(lora_dir, "adapter_model.bin"))
    print(f"Generated dummy LoRA {adapter_name} at {lora_dir} with target modules {target_modules}")

def get_lora_addresses_from_worker(model, slot_index):
    # This function will be executed on the worker via apply_model
    import os
    import torch
    from vllm.lora.layers import BaseLayerWithLoRA
    
    targets = []
    pid = os.getpid()
    
    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            lora_a_stacked = getattr(module, "lora_a_stacked", None)
            lora_b_stacked = getattr(module, "lora_b_stacked", None)
            
            if lora_a_stacked is None or lora_b_stacked is None:
                continue
                
            if isinstance(lora_a_stacked, tuple):
                for s_index in range(len(lora_a_stacked)):
                    slice_a = lora_a_stacked[s_index][slot_index]
                    slice_b = lora_b_stacked[s_index][slot_index]
                    
                    addr_a = slice_a.data_ptr()
                    size_a = slice_a.element_size() * slice_a.nelement()
                    addr_b = slice_b.data_ptr()
                    size_b = slice_b.element_size() * slice_b.nelement()
                    
                    if size_a > 0:
                        targets.append(f"{pid}:{hex(addr_a)}:{size_a}")
                    if size_b > 0:
                        targets.append(f"{pid}:{hex(addr_b)}:{size_b}")
            elif isinstance(lora_a_stacked, torch.Tensor):
                slice_a = lora_a_stacked[slot_index]
                slice_b = lora_b_stacked[slot_index]
                
                addr_a = slice_a.data_ptr()
                size_a = slice_a.element_size() * slice_a.nelement()
                addr_b = slice_b.data_ptr()
                size_b = slice_b.element_size() * slice_b.nelement()
                
                if size_a > 0:
                    targets.append(f"{pid}:{hex(addr_a)}:{size_a}")
                if size_b > 0:
                    targets.append(f"{pid}:{hex(addr_b)}:{size_b}")
    return targets

def main():
    # 1. Initialize vLLM with max_loras=1
    model_name = "facebook/opt-125m"
    print(f"Initializing vLLM with model {model_name} (max_loras=1)...")
   
    # We must set enforce_eager=True and disable_custom_all_reduce=True for CR
    llm = LLM(
        model=model_name,
        enable_lora=True,
        max_loras=1, # Crucial for testing single-slot swapping
        max_lora_rank=16,
        enforce_eager=True,
        disable_custom_all_reduce=True,
        gpu_memory_utilization=0.4
    )
    
    lora_dir_A = "/tmp/lora_A"
    lora_dir_B = "/tmp/lora_B"
    
    # Generate dummy LoRAs
    def gen_loras(model):
        generate_dummy_lora_from_model(model, lora_dir_A, "A")
        generate_dummy_lora_from_model(model, lora_dir_B, "B")
        
    llm.llm_engine.apply_model(gen_loras)
    
    # Connect to Snapshot Agent
    agent_endpoint = os.getenv("AGENT_ENDPOINT", "localhost:9001")
    print(f"Connecting to Snapshot Agent at {agent_endpoint}...")
    try:
        client = SnapshotAgentClient(endpoint=agent_endpoint)
        client.check_health()
        print("Connected to Snapshot Agent successfully.")
    except Exception as e:
        print(f"Failed to connect to Snapshot Agent: {e}")
        sys.exit(1)
        
    job_id = os.getenv("JOB_ID", "lora-test-max1")
    group = os.getenv("GROUP", "lora-group")
    backend = snapshot_agent_pb2.BACKEND_GPU_CR_MEMORY_ADDRESSES
    
    # Since max_loras=1, we only use Slot 0
    slot_index = 0
    
    # 2. Load LoRA A and run inference (loads into Slot 0)
    print("\n--- Loading LoRA A ---")
    lora_request_A = LoRARequest("lora_A", 1, lora_dir_A)
    sampling_params = SamplingParams(temperature=0.0, max_tokens=10)
    
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_initial = outputs[0].outputs[0].text
    print(f"LoRA A Initial Output: {repr(output_A_initial)}")
    
    # Find Slot 0 addresses
    targets_A = llm.llm_engine.apply_model(lambda m: get_lora_addresses_from_worker(m, slot_index))[0]
    print(f"LoRA A (Slot {slot_index}) targets: {len(targets_A)} regions found.")
    
    # 3. Snapshot LoRA A (Slot 0)
    print("\n--- Snapshotting LoRA A (Slot 0) ---")
    try:
        resp = client.snapshot_and_wait(job_id=job_id, group=group + "-A", backend=backend, memory_addresses=targets_A)
        print("Snapshot A response:", resp.status)
    except Exception as e:
        print(f"Snapshot A failed: {e}")
        sys.exit(1)
        
    # 4. Load LoRA B and run inference (overwrites Slot 0)
    print("\n--- Loading LoRA B ---")
    lora_request_B = LoRARequest("lora_B", 2, lora_dir_B)
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_B_initial = outputs[0].outputs[0].text
    print(f"LoRA B Initial Output: {repr(output_B_initial)}")
    
    # Find Slot 0 addresses again (should be same as targets_A)
    targets_B = llm.llm_engine.apply_model(lambda m: get_lora_addresses_from_worker(m, slot_index))[0]
    print(f"LoRA B (Slot {slot_index}) targets: {len(targets_B)} regions found.")
    
    # Verify targets are identical (proving they share the slot)
    if targets_A != targets_B:
        print("WARNING: Targets for A and B are different! Slot addresses are not static?")
        # We will use targets_B for B's snapshot just in case, but they should be the same.
    else:
        print("SUCCESS: Slot addresses for A and B are identical.")

    # 5. Snapshot LoRA B (Slot 0)
    print("\n--- Snapshotting LoRA B (Slot 0) ---")
    try:
        resp = client.snapshot_and_wait(job_id=job_id, group=group + "-B", backend=backend, memory_addresses=targets_B)
        print("Snapshot B response:", resp.status)
    except Exception as e:
        print(f"Snapshot B failed: {e}")
        sys.exit(1)
        
    # 6. Restore A and test (Hijack Test)
    # vLLM thinks B is active in Slot 0. We restore A's weights into Slot 0.
    # If we request B, we should get A's output.
    print("\n--- Restoring LoRA A (Slot 0) ---")
    try:
        resp = client.restore_and_wait(job_id=job_id, group=group + "-A", backend=backend, memory_addresses=targets_A)
        print("Restore A response:", resp.status)
    except Exception as e:
        print(f"Restore A failed: {e}")
        sys.exit(1)
        
    print("Running query requesting LoRA B (expecting LoRA A output)...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_after_restore_A = outputs[0].outputs[0].text
    print(f"Output (requested B): {repr(output_after_restore_A)}")
    
    if output_after_restore_A == output_A_initial:
        print("SUCCESS: LoRA A restored correctly (hijacked LoRA B request)!")
    else:
        print(f"FAILURE: LoRA A restored output mismatch! Expected A's output {repr(output_A_initial)}, got {repr(output_after_restore_A)}")
        
    # 7. Restore B and test
    print("\n--- Restoring LoRA B (Slot 0) ---")
    try:
        resp = client.restore_and_wait(job_id=job_id, group=group + "-B", backend=backend, memory_addresses=targets_B)
        print("Restore B response:", resp.status)
    except Exception as e:
        print(f"Restore B failed: {e}")
        sys.exit(1)
        
    print("Running query requesting LoRA B (expecting LoRA B output)...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_after_restore_B = outputs[0].outputs[0].text
    print(f"Output (requested B): {repr(output_after_restore_B)}")
    
    if output_after_restore_B == output_B_initial:
        print("SUCCESS: LoRA B restored correctly!")
    else:
        print(f"FAILURE: LoRA B restored output mismatch! Expected B's output {repr(output_B_initial)}, got {repr(output_after_restore_B)}")
        
    # 8. Test what happens if we request A (should trigger vLLM reload and still work, but slower)
    print("\n--- Requesting LoRA A (triggers vLLM reload) ---")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_reloaded = outputs[0].outputs[0].text
    print(f"Output (requested A): {repr(output_A_reloaded)}")
    if output_A_reloaded == output_A_initial:
         print("SUCCESS: LoRA A reloaded and worked correctly.")
    else:
         print("FAILURE: LoRA A reload failed.")

    print("\nLoRA Swap Max 1 Test Completed.")

if __name__ == "__main__":
    main()
