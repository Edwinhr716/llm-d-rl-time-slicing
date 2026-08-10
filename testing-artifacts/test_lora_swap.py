"""LoRA A/B swap test for the memory-regions backend (max_loras=2).

Two adapters live in separate vLLM LoRA slots; each slot's device regions
are snapshotted into a named snapshot slot and restored, verifying outputs
match the per-adapter baselines. Ported from the prototype's group-keyed
memory_addresses API to the upstream BackendConfig oneof (snapshot_name
names the slot; the request's group is orchestrator-owned and unused here).
"""

import json
import os
import sys

import torch
from vllm import LLM, SamplingParams
from vllm.lora.request import LoRARequest
from vllm.lora.layers import BaseLayerWithLoRA

# Add path to client library
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../pkg/client/python")))
try:
    from timeslice.snapshot_agent import SnapshotAgentClient, memory_regions_config
except ImportError:
    sys.path.append("/app/pkg/client/python")
    from timeslice.snapshot_agent import SnapshotAgentClient, memory_regions_config

FAILURES = []


def check(ok, success_msg, failure_msg):
    if ok:
        print(f"SUCCESS: {success_msg}")
    else:
        print(f"FAILURE: {failure_msg}")
        FAILURES.append(failure_msg)


def generate_dummy_lora_from_model(model, lora_dir, adapter_name):
    print(f"Generating dummy LoRA {adapter_name}...")
    os.makedirs(lora_dir, exist_ok=True)
    target_modules = []
    state_dict = {}
    rank = 16
    val_multiplier = 1.0 if adapter_name == "A" else 10.0  # Make them very different

    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            sub_modules = []
            if name.endswith(".qkv_proj"):
                base_name = name[:-8]
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
        "base_model_name_or_path": "dummy",
    }
    with open(os.path.join(lora_dir, "adapter_config.json"), "w") as f:
        json.dump(config, f, indent=2)
    torch.save(state_dict, os.path.join(lora_dir, "adapter_model.bin"))
    print(f"Generated dummy LoRA {adapter_name} at {lora_dir} with target modules {target_modules}")


def get_lora_regions_from_worker(model, slot_index):
    """Returns legacy 'pid:0xADDR:size' specs for the slot's LoRA tensors."""
    import os
    import torch
    from vllm.lora.layers import BaseLayerWithLoRA

    regions = []
    pid = os.getpid()

    for name, module in model.named_modules():
        if isinstance(module, BaseLayerWithLoRA):
            lora_a_stacked = getattr(module, "lora_a_stacked", None)
            lora_b_stacked = getattr(module, "lora_b_stacked", None)
            if lora_a_stacked is None or lora_b_stacked is None:
                continue

            stacks = (
                list(zip(lora_a_stacked, lora_b_stacked))
                if isinstance(lora_a_stacked, tuple)
                else [(lora_a_stacked, lora_b_stacked)]
            )
            for stack_a, stack_b in stacks:
                for t in (stack_a[slot_index], stack_b[slot_index]):
                    size = t.element_size() * t.nelement()
                    if size > 0:
                        regions.append(f"{pid}:{hex(t.data_ptr())}:{size}")
    return regions


def main():
    # 1. Initialize vLLM
    model_name = "facebook/opt-125m"
    print(f"Initializing vLLM with model {model_name}...")

    # We must set enforce_eager=True and disable_custom_all_reduce=True for CR
    llm = LLM(
        model=model_name,
        enable_lora=True,
        max_loras=2,
        max_lora_rank=16,
        enforce_eager=True,
        disable_custom_all_reduce=True,
        gpu_memory_utilization=0.4,
    )

    lora_dir_A = "/tmp/lora_A"
    lora_dir_B = "/tmp/lora_B"

    def gen_loras(model):
        generate_dummy_lora_from_model(model, lora_dir_A, "A")
        generate_dummy_lora_from_model(model, lora_dir_B, "B")

    llm.llm_engine.apply_model(gen_loras)

    agent_endpoint = os.getenv("AGENT_ENDPOINT", "localhost:9001")
    print(f"Connecting to Snapshot Agent at {agent_endpoint}...")
    try:
        client = SnapshotAgentClient(endpoint=agent_endpoint)
        health = client.check_health("memory-regions")
        print(f"Connected to Snapshot Agent; memory-regions backend: {health.status}")
        if health.status != "SERVING":
            sys.exit(1)
    except Exception as e:
        print(f"Failed to connect to Snapshot Agent: {e}")
        sys.exit(1)

    job_id = os.getenv("JOB_ID", "lora-test")
    slot_A = os.getenv("SLOT_A", "lora-slot-a")
    slot_B = os.getenv("SLOT_B", "lora-slot-b")

    def op(kind, slot, regions):
        fn = client.snapshot_and_wait if kind == "snapshot" else client.restore_and_wait
        resp = fn(job_id=job_id, backend_config=memory_regions_config(regions, snapshot_name=slot))
        print(f"{kind} {slot}: {resp.status} ({resp.elapsed_ms}ms)")
        if resp.status != "OPERATION_STATUS_COMPLETE":
            print(f"{kind} of {slot} failed: {resp.error}")
            sys.exit(1)

    # 2. Load LoRA A and run inference (loads into Slot 0)
    print("\n--- Loading LoRA A ---")
    lora_request_A = LoRARequest("lora_A", 1, lora_dir_A)
    sampling_params = SamplingParams(temperature=0.0, max_tokens=10)

    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_initial = outputs[0].outputs[0].text
    print(f"LoRA A Initial Output: {repr(output_A_initial)}")

    slot_A_index = 0
    targets_A = llm.llm_engine.apply_model(lambda m: get_lora_regions_from_worker(m, slot_A_index))[0]
    print(f"LoRA A (Slot {slot_A_index}) targets: {len(targets_A)} regions found.")

    # 3. Load LoRA B and run inference (loads into Slot 1)
    print("\n--- Loading LoRA B ---")
    lora_request_B = LoRARequest("lora_B", 2, lora_dir_B)
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_B_initial = outputs[0].outputs[0].text
    print(f"LoRA B Initial Output: {repr(output_B_initial)}")

    slot_B_index = 1
    targets_B = llm.llm_engine.apply_model(lambda m: get_lora_regions_from_worker(m, slot_B_index))[0]
    print(f"LoRA B (Slot {slot_B_index}) targets: {len(targets_B)} regions found.")

    check(
        output_A_initial != output_B_initial,
        "Initial outputs are different.",
        "Outputs are identical! LoRA weights might not have had enough effect.",
    )

    print("\n--- Snapshotting LoRA A (Slot 0) ---")
    op("snapshot", slot_A, targets_A)

    print("\n--- Restoring LoRA A (to remap memory for B) ---")
    op("restore", slot_A, targets_A)

    print("\n--- Snapshotting LoRA B (Slot 1) ---")
    op("snapshot", slot_B, targets_B)

    # 5. Restore A and test
    print("\n--- Restoring LoRA A (Slot 0) ---")
    op("restore", slot_A, targets_A)

    print("Running query for LoRA A...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_A)
    output_A_restored = outputs[0].outputs[0].text
    print(f"LoRA A Restored Output: {repr(output_A_restored)}")
    check(
        output_A_restored == output_A_initial,
        "LoRA A restored correctly!",
        f"LoRA A restored output mismatch! Expected {repr(output_A_initial)}, got {repr(output_A_restored)}",
    )

    # 6. Restore B and test. Tenant-B's first generated token after restoring
    # A is the 2026-08-04 co-resident-corruption regression check.
    print("\n--- Restoring LoRA B (Slot 1) ---")
    op("restore", slot_B, targets_B)

    print("Running query for LoRA B...")
    outputs = llm.generate(["Who are you?"], sampling_params, lora_request=lora_request_B)
    output_B_restored = outputs[0].outputs[0].text
    print(f"LoRA B Restored Output: {repr(output_B_restored)}")
    check(
        output_B_restored == output_B_initial,
        "LoRA B restored correctly!",
        f"LoRA B restored output mismatch! Expected {repr(output_B_initial)}, got {repr(output_B_restored)}",
    )

    if FAILURES:
        print(f"\nLoRA Swap Test FAILED ({len(FAILURES)} failures):")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("\nLoRA Swap Test PASSED.")


if __name__ == "__main__":
    main()
